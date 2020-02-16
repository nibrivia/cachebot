from flask import Flask, request
import json
#from wekzeug.utils import secure_filename
app = Flask(__name__)

class Coordinator:
    def __init__(self):
        self.count = 0
        self.queue = []
        self.hosts   = dict()
        self.workers = dict()

    def add_job(self, params):
        self.queue.append(params)

    def worker_done(self, params):
        worker_id = params["hostname"] + params["worker_id"]
        del self.workers[worker_id]

    def check_in(self, params):
        worker_id = params["hostname"] + params["worker_id"]

        # They should exist
        assert worker_id in self.workers, self.workers

        # The job ids should match
        job = self.workers[worker_id]
        assert job["job_id"] == params["job_id"], \
                "Got <%s>, expected <%s>" % (params["job_id"], job["job_id"])

        job["memory"] = float(params["memory"])


    def get_job(self, params):
        worker_id = params["hostname"] + params["worker_id"]

        # Should not already be running something
        if worker_id in self.workers:
            assert False, "worker already has job"

        # If we have something, give it
        if self.queue:
            self.count += 1
            job_id = self.count
            job = dict(
                    job_id    = str(job_id),
                    hostname  = params["hostname"],
                    worker_id = params["worker_id"],
                    params    = self.queue.pop(0))
            self.workers[worker_id] = dict(**job, memory = 0)

            return job

C = Coordinator()
for i in range(100):
    C.add_job("job-%02d" % i)

@app.route('/')
def hello_world():
    return "You probably shouldn't be here, go away..."

@app.route("/get-job", methods=['POST'])
def get_job():
    job = C.get_job(request.form)
    resp = dict()
    if job is not None:
        resp["job"] = job
    return resp

@app.route("/check-in", methods=['POST'])
def check_in():
    C.check_in(request.form)
    return 'OK'

@app.route("/job-done", methods=['POST'])
def job_done():
    C.worker_done(request.form)
    if False:
        f = request.files['upload']
        f.save('data/' + secure_filename(f.filename))
    return 'OK'

