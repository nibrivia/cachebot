import requests
import concurrent.futures
import sys, os, time, subprocess, psutil

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

        # Named args need pasting and splitting
        baseline = ["singularity", "run", "netsim.sif"]
        args = " ".join(["--%s %s" % (k, v) for k, v in job["params"].items()]).split()

        # Run it
        proc = subprocess.Popen(baseline + args, stdout = subprocess.DEVNULL)
        #proc = subprocess.Popen(["sleep", "10"])

        # Get some info, will be useful later
        pid = proc.pid
        ps  = psutil.Process(pid)
        print("%s: [%d] " % (self.worker_id, pid) + " ".join(baseline+args))

        # Wait and do check-ins
        while True:
            try:
                r = proc.wait(timeout = 2)
                print("%s: [%d] done, return code %s" % (self.worker_id, pid, r))
                break
            except:
                memory_usage = ps.memory_info().rss
                resp = requests.post(
                        SERVER + "/check-in",
                        data = dict(**self.worker_params,
                                    job_id = job["job_id"],
                                    memory = memory_usage)
                            )
                if resp.text != "OK":
                    print("%s: Server responded %s, exiting" % (self.worker_id, resp.text))
                    sys.exit(-1)


        # DONE, upload results
        resp = requests.post(SERVER + "/job-done",
                data = dict(**self.worker_params,
                            return_code = r))
        if resp.text != "OK":
            print(resp.text)
            print("Server responded %s, exiting" % resp.text)
            sys.exit(-1)



    def start(self):
        """This function tries to get a job, when it does, goes to run"""
        print("Starting worker %s" % self.worker_id)
        while True:
            # Get job
            try:
                response = requests.post(SERVER + "/get-job",
                        data = self.worker_params)
            except:
                # Server not up, something
                time.sleep(10)
                continue

            if response.status_code == 200 and "job" in response.json():
                # Got a job, do it
                job = response.json()["job"]
                self.run_job(job)
            else:
                # Wait and try again
                try:
                    wait_time = int(response.json()["wait"])
                    time.sleep(wait_time)
                except:
                    time.sleep(5)

            # This guarantees we at least wait 1s between requests
            time.sleep(1)

def local_coordinator(max_jobs):
    """Starts local workers and catches them when they fail"""
    workers = [Worker(i, HOSTNAME) for i in range(max_jobs)]
    with concurrent.futures.ProcessPoolExecutor(max_workers = max_jobs) as executor:
        for i, w in enumerate(workers):
            f = executor.submit(workers[i].start)
            f.add_done_callback(worker_exit)

def worker_exit(f):
    """Gets called on a worker terminating, tries to determine the cause and emails out??"""
    # TODO email out
    print("A worker terminated...")
    print(f.result())

def update_sif():
    """This will be run everytime, re-run the singularity def file if need be"""
    p = subprocess.run("singularity inspect --deffile netsim.sif | diff -B - netsim.def", shell = True)
    if p.returncode != 0:
        # Needs refreshing
        # Download file and try again
        print(".sif not up-to-date, no update mechanism, abort")
        return False
    return True

def check_install():
    """Make sure everything works, or die trying"""
    # Check if this is a directory first, clone if need be
    # Make this not username depndent
    os.chdir("/home/nibr/sim-worker/")

    # Check .sif
    if not update_sif():
        return False

    return True

if __name__ == "__main__":
    if not check_install():
        print("Install didn't check out, aborting")
        sys.exit(-1)

    local_coordinator(max_jobs = 2)
