import requests
import time
import concurrent.futures

import socket
HOSTNAME = socket.gethostname()

import multiprocessing
N_CPUS   = multiprocessing.cpu_count()


SERVER = "http://localhost:5000"

def get_and_run(worker_id):
    while True:
        # Get job
        response = requests.post(SERVER + "/get-job", data = dict(worker_id = worker_id))

        if "job" in response.json():
            # Got a job, do it
            job = response.json()["job"]
            print("{hostname}: starting job {job_id}".format(**job))
            time.sleep(100)
        else:
            # Wait and try again
            print("%s: no job, trying again later" % worker_id)
            time.sleep(5)

def local_coordinator(max_jobs):
    with concurrent.futures.ProcessPoolExecutor(max_workers = max_jobs) as executor:
        for i in range(max_jobs):
            worker_id = "%s/%02d" % (HOSTNAME, i)
            executor.submit(get_and_run, worker_id = worker_id)


if __name__ == "__main__":
    local_coordinator(max_jobs = N_CPUS)
