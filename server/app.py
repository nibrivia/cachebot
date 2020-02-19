from flask import Flask, request, url_for
from secrets import notify_url
import json, requests, time, os
import uuid
from werkzeug.utils import secure_filename
app = Flask(__name__)
#ALLOWED_EXTENSIONS = {'csv'}
UPLOAD_FOLDER = "/home/nibr/rotorsim/data/"

class Coordinator:
    def __init__(self):
        self.count = 0
        self.queue = []
        self.jobs    = dict()
        self.workers = dict()

        self.last_status_check = 0
        self.check_in_period   = 4

    def add_job(self, params):
        self.status_check()
        job_id = str(uuid.uuid4())
        params["uuid"] = job_id
        self.queue.append(dict(
            params = params,
            job_id = job_id))

        return job_id

    def internal_worker_id(self, hostname, worker_id):
        return (hostname, worker_id)

    def status(self, raw = False):
        status = dict(queue = self.queue, jobs = self.jobs, workers = self.workers)
        if not raw:
            return {k: len(v) for k, v in status.items()}
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
        if time.time() - self.last_status_check < 1:
            return
        #print("status check")
        self.last_status_check = time.time()

        to_remove = []
        for worker, status in self.workers.items():
            if time.time() - status["last-check-in"] > self.check_in_period * 3.2:
                to_remove.append(worker)

        for inactive_id in to_remove:
            print(inactive_id, "inactive")
            self.notify_slack("Worker %s has gone silent" % inactive_id)
            self.job_failed(inactive_id)
            del self.workers[inactive_id]


    def worker_active(self, worker_id):
        if worker_id not in self.workers:
            self.notify_slack("%s came online :)" % worker_id)
            self.workers[worker_id] = dict()
        self.workers[worker_id]["last-check-in"] = time.time()

        self.status_check()


    def job_str(self, job):
        pretty_params = " ".join("`%s %s`" % i for i in job["params"].items())
        duration = time.time() - job["start"]
        slack_message = "`%s` took %ds and %.3fGB: %s" % (job["job_id"], duration, job["memory"]/1e9, pretty_params)
        return slack_message

    def worker_done(self, hostname, worker_id, return_code):
        worker_id = hostname + worker_id
        self.worker_active(worker_id)
        job = self.jobs[worker_id]


        if int(return_code) != 0:
            self.job_failed(worker_id)
        else:
            # Notify slack
            #self.notify_slack(self.job_str(job))
            del self.jobs[worker_id]
        return job["job_id"]

    def job_failed(self, worker_id):
        # We might be asked to fail jobs we don't have
        if worker_id not in self.jobs:
            return

        job = self.jobs[worker_id]
        print("%s failed" % worker_id)
        if "failed" in job:
            print("%s has already been rescheduled, aborting" % worker_id)
            self.notify_slack("FAILED after 1 retry on %s: %s" % (worker_id, self.job_str(job)))
        else:
            self.notify_slack("FAILED on %s: %s" % (worker_id, self.job_str(job)))
            self.queue.append(dict(
                job_id = job["job_id"],
                params = job["params"],
                failed = True)) # Only put the job back
        del self.jobs[worker_id]

    def check_in(self, hostname, worker_id, job_id, memory):
        worker_id = hostname + worker_id
        self.worker_active(worker_id)

        # They should exist
        assert worker_id in self.jobs, self.jobs

        # The job ids should match
        job = self.jobs[worker_id]
        assert job["job_id"] == job_id, \
                "Got <%s>, expected <%s>" % (job_id, job["job_id"])

        job["memory"] = float(memory)
        job["last-check-in"] = time.time()

        return "OK"


    def get_job(self, hostname, worker_id):
        worker_id = hostname + worker_id
        self.worker_active(worker_id)

        # Should not already be running something
        if worker_id in self.jobs:
            self.job_failed(worker_id)

        # Nothing to do, we're done
        if not self.queue:
            return dict(wait = 4)

        # Assign new job
        self.count += 1
        job = self.queue.pop(0)
        job["start"]     = time.time()
        job["hostname"]  = hostname
        job["worker_id"] = worker_id

        self.jobs[worker_id] = dict(**job, memory = 0)
        job["last-check-in"] = time.time()

        return dict(job = job)

C = Coordinator()
C.add_job(dict(
    time_limit = 3,
    n_tor      = 65,
    n_switches = 37,
    n_cache    = 16,
    n_xpand    =  5,
    workload   = "chen"
    ))

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
    C.check_in(**request.form)
    return 'OK'

with app.test_request_context():
    url_for('static', filename="netsim.sif")


@app.route("/job-done", methods=['POST'])
def job_done():
    job_id = C.worker_done(**request.form)
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
