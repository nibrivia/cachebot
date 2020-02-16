import requests
import time
import concurrent.futures

import socket
HOSTNAME = socket.gethostname()

import multiprocessing
N_CPUS   = multiprocessing.cpu_count()

SERVER = "http://localhost:5000"

def run_job(job_desc):
    """This functions deals with all the logistics of actually running a job"""

    print("{hostname}: starting job {job_id}".format(**job))
    # Start job
    time.sleep(100)

    # DONE, upload results
    requests.post(SERVER + "/job-done", data = dict(worker_id = worker_id))

def get_job(worker_id):
    """This function tries to get a job, when it does, goes to run"""
    while True:
        # Get job
        response = requests.post(SERVER + "/get-job", data = dict(worker_id = worker_id))

        if "job" in response.json():
            # Got a job, do it
            job = response.json()["job"]
            run(job)
        else:
            # Wait and try again
            print("%s: no job, trying again later" % worker_id)
            time.sleep(5) # TODO some randomness

def local_coordinator(max_jobs):
    with concurrent.futures.ProcessPoolExecutor(max_workers = max_jobs) as executor:
        for i in range(max_jobs):
            worker_id = "%s/%02d" % (HOSTNAME, i)
            executor.submit(get_and_run, worker_id = worker_id)


if __name__ == "__main__":
    local_coordinator(max_jobs = N_CPUS)
