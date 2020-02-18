from flask import Flask, request, url_for
from secrets import notify_url
import json, requests
import uuid
#from wekzeug.utils import secure_filename
app = Flask(__name__)

class Coordinator:
    def __init__(self):
        self.count = 0
        self.queue = []
        self.hosts   = dict()
        self.workers = dict()

    def add_job(self, params):
        self.queue.append(dict(**params)) #, job_id = str(uuid.uuid4())))

    def internal_worker_id(self, hostname, worker_id):
        return (hostname, worker_id)

    def worker_done(self, hostname, worker_id, return_code):
        worker_id = hostname + worker_id

        # Notify slack
        try:
            job = self.workers[worker_id]
            params = dict(text = "%s done with job #%s: `%s`" % (worker_id, job["job_id"], job["params"]))
            r = requests.post(notify_url, data = json.dumps(params))
        except:
            # If we fail, whatever
            pass

        if int(return_code) != 0:
            self.job_failed(worker_id)

        print("%s done" % worker_id)
        del self.workers[worker_id]

    def job_failed(self, worker_id):
        job = self.workers[worker_id]
        self.queue.append(job["params"]) # Only put the params back
        print("%s failed" % worker_id)

    def check_in(self, hostname, worker_id, job_id, memory):
        worker_id = hostname + worker_id

        # They should exist
        assert worker_id in self.workers, self.workers

        # The job ids should match
        job = self.workers[worker_id]
        assert job["job_id"] == job_id, \
                "Got <%s>, expected <%s>" % (job_id, job["job_id"])

        job["memory"] = float(memory)
        print(memory)

        return "OK"


    def get_job(self, hostname, worker_id):
        worker_id = hostname + worker_id

        # Should not already be running something
        if worker_id in self.workers:
            self.job_failed(worker_id)

        # Nothing to do, we're done
        if not self.queue:
            return dict(wait = 4)

        # Assign new job
        self.count += 1
        job_id = self.count
        job = dict(
                job_id    = str(job_id),
                hostname  = hostname,
                worker_id = worker_id,
                params    = self.queue.pop(0))
        self.workers[worker_id] = dict(**job, memory = 0)

        return dict(job = job)

C = Coordinator()
C.add_job(dict(
    time_limit = 30,
    n_tor      = 129,
    n_switches = 37,
    n_cache    = 16,
    n_xpand    =  5,
    workload   = "chen"
    ))

@app.route('/')
def hello_world():
    return "You probably shouldn't be here, go away..."

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
    C.worker_done(**request.form)
    if False:
        f = request.files['upload']
        f.save('data/' + secure_filename(f.filename))
    return 'OK'

