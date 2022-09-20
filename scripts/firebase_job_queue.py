import json
import os

import pyrebase
import time
from firebase_config import config
from firebase_config import host_config
import power_manager
import wmi
# Requirements
# collections
# pyrebase
# urllib3

class FirebaseJobQueue:
    busy = False
    localjobqueue = []
    localjobmap = dict()
    on_begin_job = None
    inhibitor = power_manager.WindowsInhibitor()

    def __init__(self):
        self.config = config
        self.hostname = host_config['hostname']
        self.active_job = None

    def monitor_jobs(self):
        self.firebase = pyrebase.initialize_app(config)
        self.auth = self.firebase.auth().sign_in_with_email_and_password("firebasejobqueue@doubtech.com", config["apiKey"])
        self.idToken = self.auth['idToken']
        self.log ("Logged in.")
        self.db = self.firebase.database()
        self.db.child("jobs").child("queue").stream(self.on_jobs_changed, self.idToken)
        self.db.child("jobs").child("ping").stream(self.pong, self.idToken)
        self.ping()

        if os.path.exists('osinfo.json'):
            with open('osinfo.json', 'r') as f:
                osinfo = json.load(f)
        else:
            computer = wmi.WMI()
            computer_info = computer.Win32_ComputerSystem()[0]
            os_info = computer.Win32_OperatingSystem()[0]
            proc_info = computer.Win32_Processor()[0]
            gpu_info = computer.Win32_VideoController()[0]

            os_name = os_info.Name.encode('utf-8').split(b'|')[0]
            os_version = ' '.join([os_info.Version, os_info.BuildNumber])
            system_ram = int(round(float(os_info.TotalVisibleMemorySize) / 1048576))  # KB to GB))

            osinfo = {
                "os": "%s" % os_name,
                "cpu": "%s" % proc_info.Name,
                "gpu": "%s" % gpu_info.Name,
                "ram": "%s" % system_ram
            }
            with open('osinfo.json', 'w') as f:
                json.dump(osinfo, f)

        self.db.child("jobs").child("nodes").child(self.hostname).child("system-info").set(osinfo, self.idToken)
        self.db.child("jobs").child("nodes").child(self.hostname).child("current-job").set("", self.idToken)

    def pong(self, response):
        print (response)
        self.ping_time = response['data']
        self.db.child("jobs").child("nodes").child(self.hostname).child("ping").set(self.ping_time, self.idToken)

    def ping(self):
        self.ping_time = time.time()
        self.db.child("jobs").child("ping").set(time.time(), self.idToken)


    # Write data
    # data = {"prompt": "Gandalf the Grey riding a horse or two"}
    # db.child("jobs").push(data)

    def get_avail_node(self, job):
        return self.get_queue_node(job).child("available-nodes").child(self.hostname)

    def get_queue_node(self, job):
        return self.db.child("jobs").child("queue").child(job["name"])

    def log(self, message, job=None):
        if job is None:
            print ("[JOB QUEUE] %s" % message)
        else:
            print ("[JOB QUEUE - %s] %s" % (job["name"], message))

    def announce(self, job, available):
        self.log("Announcing availability for job is %s to %s" % (available if 'available' else 'busy', self.get_avail_node(job).path), job)
        if 'available-nodes' not in job or\
                self.hostname not in job['available-nodes'] or\
                job['available-nodes'][self.hostname] != available:
            if "available-nodes" not in job:
                job['available-nodes'] = {}
            job['available-nodes'][self.hostname] = available
            self.get_avail_node(job).set(available, self.idToken)

    def announce_processing(self, job, busy):
        self.log("Announcing availability for job is %s" % busy, job)
        self.update_state(job, "processing")

    def job(self, job, overwrite=False):
        if overwrite or job["name"] not in self.localjobmap:
            self.localjobmap[job["name"]] = job
        return job

    def next_job(self):
        if len(self.localjobqueue) > 0:
            job = self.localjobmap[self.localjobqueue[0]]
            self.active_job = job
            return job
        return None

    def dequeue(self, job):
        if self.is_queued(job):
            self.localjobqueue.remove(job["name"])
            del self.localjobmap[job["name"]]
            if self.active_job == self.job(job):
                self.active_job = None

    def queue(self, job):
        try:
            if 'name' in job and job['name'] not in self.localjobqueue and 'state' not in job:
                self.job(job, True)
                self.localjobqueue.append(job["name"])
        except Exception as e:
            self.log("Error queuing job: %s" % e, job)

    def is_queued(self, job):
        return 'name' in job and job["name"] in self.localjobqueue

    def handle_work(self, job):
        self.log("Handling work for job", job)
        if 'state' in job:
            if not self.handle_error_state(job):
                self.log("Job is already being processed, nothing to handle.", job)
        elif not self.is_queued(job):
            self.log("Handling work for job", job)
            #self.ping()
            if 'state' in job and job['state'] == "complete":
                return True
            elif not self.busy:
                self.announce(job, True)
            if 'worker' in job and job['worker'] == self.hostname:
                self.processing(job)
                return True
            elif 'worker' not in job:
                self.processing(job)
                return True
        return False

    def update_state(self, job, state):
        if 'state' not in job or job['state'] != state:
            job['state'] = state
            self.get_queue_node(job).child("state").set(state, self.idToken)

    def handle_error_state(self, job):
        if 'state' in job:
            if 'worker' in job and job['worker'] == self.hostname \
                    and self.active_job is not None \
                    and 'name' in self.active_job \
                    and self.active_job['name'] != job['name']:
                self.log("Found what appears to be a dead job, marking it as failed.", job)
                self.update_state(job, "failed")

            self.job_complete(job, job['state'])
            return True
        return False
    def processing(self, job):
        try:
            if self.handle_error_state(job): return

            self.queue(job)

            if not self.busy:
                if 'worker' not in job:
                    self.log("Job is not assigned to a worker, picking up the work since I'm idle.r.", job)
                    self.get_queue_node(job).child("worker").set(self.hostname, self.idToken)
                self.db.child("jobs").child("nodes").child(self.hostname).child("current-job").set(job["name"], self.idToken)
                self.log ("Processing for job has begun.", job)
                job = self.next_job()
                if job is not None:
                    self.begin_job(job)
                else:
                    self.log ("No jobs left to process.")
            else:
                self.log("System is currently processing %s." % self.active_job["name"], job)
        except Exception as e:
            self.job_complete(job, "failed")

    def begin_job(self, job):
        self.inhibitor.inhibit()
        self.busy = True
        self.announce(job, False)
        self.announce_processing(job, True)
        self.active_job = job
        self.on_begin_job(job)

    def update_job(self, job):
        self.get_queue_node(job).set(job, self.idToken)

    def job_complete(self, job, status="complete"):
        try:
            self.busy = False
            self.active_job = None
            self.inhibitor.uninhibit()
            self.dequeue(job)
            self.get_queue_node(job).set(job, self.idToken)
            self.get_avail_node(job).remove(self.idToken)
            self.update_state(job, status)
            self.db.child("jobs").child("nodes").child(self.hostname).child("current-job").set("", self.idToken)
            self.db.child("jobs").child("nodes").child(self.hostname).child("last-job").set(job['name'], self.idToken)

            self.log("Job complete", job)
        except Exception as e:
            self.log("Job failed %s" % e, job)
        if len(self.localjobqueue) > 0:
            self.processing(self.localjobqueue[0])

    def on_jobs_changed(self, response):
        data = response['data']
        path = [x for x in response['path'].split('/') if x]
        if data is None:
            return

        if len(path) == 0:
            for job_name in data:
                job = data[job_name]
                if "name" not in job:
                    job["name"] = job_name
                    self.db.child("jobs").child("queue").child(job_name).child("name").set(job_name, self.idToken)

                self.log ("Updating state job state", job)
                self.handle_work(job)
        elif path is not None and path != "/":
            job_name = path[0]
            job = self.db.child("jobs").child("queue").child(job_name).get().val()
            if job is not None:
                if "name" not in job:
                    job["name"] = job_name
                    self.db.child("jobs").child("queue").child(job_name).child("name").set(job_name, self.idToken)

                self.log ("Received job", job)
                self.handle_work(job)
            else:
                self.log("Job %s not found in queue." % job_name)


def simulate_complete(jobqueue, job):
    job["url"] = "http://www.google.com"
    jobqueue.job_complete(job)

if __name__ == '__main__':
    jobqueue = FirebaseJobQueue()
    jobqueue.on_begin_job = lambda job: simulate_complete(jobqueue, job)

    jobqueue.monitor_jobs()
    while True:
        pass
