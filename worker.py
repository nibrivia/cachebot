import requests
import time
import concurrent.futures
import subprocess
import sys

# Worker constants
import socket
import multiprocessing
N_CPUS   = multiprocessing.cpu_count()
HOSTNAME = socket.gethostname()
SERVER   = "http://localhost:5000"

class Worker:
    def __init__(self, worker_id, hostname):
        self.worker_id = worker_id
        self.hostname  = hostname

        self.worker_params = dict(worker_id = worker_id, hostname = hostname)

    def send_req(self, path):
        resp = requests.post(SERVER + "/check-in", data = self.worker_params)
        if resp.text != "OK":
            print("%s: Server responded %s, exiting" % (self.worker_id, resp.text))
            sys.exit(-1)


    def run_job(self, job):
        """This functions deals with all the logistics of actually running a job"""
        print("%s: starting job %s" % (self.worker_id, job))

        # Start job
        self.job = job
        proc = subprocess.Popen(["/bin/sleep", "10"])
        print("%s: job started" % self.worker_id)

        # Wait and do check-ins
        while True:
            try:
                proc.wait(timeout = 2)
                break
            except:
                print("%s: check-in!" % self.worker_id)
                resp = requests.post(
                        SERVER + "/check-in",
                        data = dict(**self.worker_params,
                                    job_id = job["job_id"],
                                    memory = 0)
                            )
                if resp.text != "OK":
                    print("%s: Server responded %s, exiting" % (self.worker_id, resp.text))
                    sys.exit(-1)


        # DONE, upload results
        print("%s: job done" % self.worker_id)
        resp = requests.post(SERVER + "/job-done", data = self.worker_params)
        if resp.text != "OK":
            print(resp.text)
            print("Server responded %s, exiting" % resp.text)
            sys.exit(-1)


    def start(self):
        """This function tries to get a job, when it does, goes to run"""
        print("Starting worker %s" % self.worker_id)
        while True:
            # Get job
            response = requests.post(SERVER + "/get-job",
                    data = self.worker_params)

            if response.status_code == 200 and "job" in response.json():
                # Got a job, do it
                job = response.json()["job"]
                self.run_job(job)
            else:
                # Wait and try again
                print("%s: no job, trying again later" % self.worker_id)
                time.sleep(5) # TODO some randomness

def local_coordinator(max_jobs):
    workers = [Worker(i, HOSTNAME) for i in range(max_jobs)]
    with concurrent.futures.ProcessPoolExecutor(max_workers = max_jobs) as executor:
        for i, w in enumerate(workers):
            f = executor.submit(workers[i].start)
            f.add_done_callback(worker_exit)

def worker_exit(f):
    print("A worker terminated...")
    print(f.result())


if __name__ == "__main__":
    local_coordinator(max_jobs = 1)
