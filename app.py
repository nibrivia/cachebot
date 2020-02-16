from flask import Flask, request
import json
#from wekzeug.utils import secure_filename
app = Flask(__name__)

class Coordinator:
    def __init__(self):
        self.count = 0
        self.queue = []
        self.nodes = dict()

    def add_job(self, params):
        self.queue.append(params)

    def get_job(self, node):
        if node["worker_id"] in self.nodes:
            # Update the counts, check jobs
            pass
        else:
            # Create entry
            self.nodes[node["worker_id"]] = node

        if self.queue:
            self.count += 1
            job = dict(
                    job_id = self.count,
                    hostname = node["worker_id"],
                    params   = self.queue.pop(0))
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

@app.route("/job-done")
def job_done():
    f = request.files['upload']
    f.save('data/' + secure_filename(f.filename))

