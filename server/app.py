from flask import Flask, request, url_for
from secrets import notify_url
import json, requests, time, os
from queue import Queue
from threading import Lock, RLock
import uuid
from werkzeug.utils import secure_filename
app = Flask(__name__)
#ALLOWED_EXTENSIONS = {'csv'}
UPLOAD_FOLDER = "/home/nibr/rotorsim/data/"

class Coordinator:
    def __init__(self):
        self.count = 0
        self.queue = Queue()
        self.workers = dict()

        self.lock = Lock()

        self.lock_jobs = RLock()
        self.jobs    = dict()

        self.last_status_check = 0

    @property
    def check_in_period(self):
        return max(len(self.workers)/20, 1)

    def add_job(self, usr_params):
        self.status_check()

        job_id = str(uuid.uuid4())
        params = {k: v for k, v in usr_params.items()}
        params["uuid"] = job_id
        job_desc = dict(
            params = params,
            job_id = job_id)
        print(job_desc)

        self.queue.put(job_desc)


        return job_id

    def internal_worker_id(self, hostname, worker_id):
        return (hostname, worker_id)

    def status(self, raw = False):
        if not raw:
            host_count = dict()
            for w_id, w in self.workers.items():
                h = w["hostname"]
                host_count[h] = host_count.get(h, 0) + 1

            short_status = dict()
            short_status["n_workers"] = len(self.workers)
            short_status["workers"] = host_count
            short_status["queue"] = self.queue.qsize()
            short_status["jobs"] = len(self.jobs)

            return short_status

        status = dict(queue = [], jobs = self.jobs, workers = self.workers)
        status["queue"] = None
        return status

    def notify_slack(self, message):
        try:
            params = dict(text = message)
            r = requests.post(notify_url, data = json.dumps(params))
        except:
            # If we fail, whatever
            pass

    def status_check(self):
        # Avoid doing this too often
        if time.time() - self.last_status_check < 10:
            return
        self.lock.acquire()
        self.lock_jobs.acquire()
        if time.time() - self.last_status_check < 10:
            return
        #print("status check")
        try:
            self.last_status_check = time.time()

            to_remove = []
            for worker, status in self.workers.items():
                if time.time() - status["last-check-in"] > max(self.check_in_period * 3.2, 60):
                    # Skip if we're uploading
                    if worker in self.jobs and "uploading" in self.jobs[worker]:
                        continue
                    to_remove.append(worker)

            for inactive_id in to_remove:
                print(inactive_id, "inactive")
                self.job_failed(inactive_id, "inactive")
                del self.workers[inactive_id]
            if to_remove:
                self.notify_slack("%d worker(s) down" % (len(to_remove)))
        except Exception as e:
            print(e)
        finally:
            self.lock.release()
            self.lock_jobs.release()


    def worker_active(self, worker_id, hostname):
        if worker_id not in self.workers:
            #self.notify_slack("%s came online :)" % worker_id)
            self.workers[worker_id] = dict()
            self.workers[worker_id]["hostname"]  = hostname
        self.workers[worker_id]["last-check-in"] = time.time()

        self.status_check()


    def job_str(self, job):
        pretty_params = " ".join("`%s %s`" % i for i in job["params"].items())
        duration = time.time() - job["start"]
        slack_message = "`%s` took %ds and %.3fGB: %s" % (job["job_id"], duration, job["memory"]/1e9, pretty_params)
        return slack_message

    def start_upload(self, hostname, worker_id):
        worker_id = hostname + worker_id
        self.worker_active(worker_id, hostname)
        self.lock_jobs.acquire()
        try:
            self.jobs[worker_id]["uploading"] = True
        finally:
            self.lock_jobs.release()
        return 'OK'

    def worker_done(self, hostname, worker_id, return_code):
        worker_id = hostname + worker_id
        self.worker_active(worker_id, hostname)
        self.lock_jobs.acquire()
        try:
            job = self.jobs[worker_id]


            if int(return_code) != 0:
                self.job_failed(worker_id, "non-zero return code")
            else:
                del self.jobs[worker_id]
        finally:
            self.lock_jobs.release()

        # Notify slack
        if len(self.jobs) == 0:
            self.notify_slack("All jobs done!")

        return job["job_id"]

    def job_failed(self, worker_id, reason):
        # We might be asked to fail jobs we don't have
        self.lock_jobs.acquire()
        try:
            if worker_id not in self.jobs:
                return

            job = self.jobs[worker_id]
            print("%s failed (%s)" % (worker_id, reason))
            if "failed" in job:
                print("%s has already been rescheduled, aborting" % worker_id)
                self.notify_slack("FAILED after 1 retry on %s (%s): %s" % (worker_id, reason, self.job_str(job)))
            else:
                self.notify_slack("FAILED on %s (%s): %s" % (worker_id, reason, self.job_str(job)))
                assert job["job_id"] == job["params"]["uuid"], (job["job_id"], job["params"]["uuid"])
                self.queue.put(dict(
                    job_id = job["job_id"],
                    params = dict(**job["params"]),
                    failed = True)) # Only put the job back
            del self.jobs[worker_id]
        finally:
            self.lock_jobs.release()

    def check_in(self, hostname, worker_id, job_id, memory):
        worker_id = hostname + worker_id
        self.worker_active(worker_id, hostname)

        # They should exist
        self.lock_jobs.acquire()
        try:
            assert worker_id in self.jobs, self.jobs

            # The job ids should match
            job = self.jobs[worker_id]
            assert job["job_id"] == job_id, \
                    "Got <%s>, expected <%s>" % (job_id, job["job_id"])

            job["memory"] = float(memory)
            job["last-check-in"] = time.time()
        finally:
            self.lock_jobs.release()

        return dict(wait = self.check_in_period)


    def get_job(self, hostname, worker_id):
        worker_id = hostname + worker_id
        self.worker_active(worker_id, hostname)

        # Should not already be running something
        self.lock_jobs.acquire()
        try:
            if worker_id in self.jobs:
                self.job_failed(worker_id, "Worker requested new work")

            if self.queue.empty():
                return dict(wait = self.check_in_period)

            try:
                # Try to get job, raises exception if empty
                job = self.queue.get()
            except:
                # Nothing to do, we're done
                return dict(wait = self.check_in_period)

            # Assign new job
            self.count += 1

            job["start"]     = time.time()
            job["hostname"]  = hostname
            job["worker_id"] = worker_id
            assert job["job_id"] == job["params"]["uuid"], (job["job_id"], job["params"]["uuid"])

            self.jobs[worker_id] = dict(**job, memory = 0)
            job["last-check-in"] = time.time()
        finally:
            self.lock_jobs.release()

        print("queueing %s on %s" % (job["job_id"], worker_id))
        return dict(job = job)

C = Coordinator()
start_job =  dict(
    time_limit = 3,
    n_tor      = 65,
    n_switches = 37,
    n_cache    = 16,
    n_xpand    =  5,
    workload   = "chen"
    )

for i in range(20):
    C.add_job(start_job)

@app.route('/')
def hello_world():
    C.status_check()
    return C.status(raw = True)

@app.route("/submit-job", methods = ['POST'])
def submit_job():
    pass

@app.route("/get-job", methods=['POST'])
def get_job():
    return C.get_job(**request.form)

@app.route("/check-in", methods=['POST'])
def check_in():
    return C.check_in(**request.form)

with app.test_request_context():
    url_for('static', filename="netsim.sif")

@app.route("/start-upload", methods=['POST'])
def start_upload():
    return C.start_upload(**request.form)

@app.route("/job-done", methods=['POST'])
def job_done():
    job_id = C.worker_done(**request.form)
    if False:
        f = request.files['result']
        f.save(os.path.join(UPLOAD_FOLDER, secure_filename("done-" + str(job_id) + ".csv")))
    return 'OK'

@app.route("/slack-command", methods=['POST'])
def slack_command():
    C.status_check()
    if request.form["text"] == "help":
        return "https://github.com/nibrivia/rotorsim"
    if request.form["text"] == "status":
        return C.status()
    tokens = request.form["text"].split()
    params = dict(zip(tokens[::2], tokens[1::2]))
    job_id = C.add_job(params)
    return 'Queued with id `%s`' % job_id
