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

    def monitor_jobs(self):
        self.firebase = pyrebase.initialize_app(config)
        self.auth = self.firebase.auth().sign_in_with_email_and_password("firebasejobqueue@doubtech.com", config["apiKey"])
        self.idToken = self.auth['idToken']
        print (self.auth)
        print ("Logged in.")
        self.db = self.firebase.database()
        self.db.child("jobs").child("queue").stream(self.on_jobs_changed, self.idToken)
        self.db.child("jobs").child("ping").stream(self.pong, self.idToken)
        self.ping()

        computer = wmi.WMI()
        computer_info = computer.Win32_ComputerSystem()[0]
        os_info = computer.Win32_OperatingSystem()[0]
        proc_info = computer.Win32_Processor()[0]
        gpu_info = computer.Win32_VideoController()[0]

        os_name = os_info.Name.encode('utf-8').split(b'|')[0]
        os_version = ' '.join([os_info.Version, os_info.BuildNumber])
        system_ram = float(os_info.TotalVisibleMemorySize) / 1048576  # KB to GB

        self.db.child("jobs").child("nodes").child(self.hostname).child("os").set(os_name, self.idToken)
        self.db.child("jobs").child("nodes").child(self.hostname).child("cpu").set(proc_info.Name, self.idToken)
        self.db.child("jobs").child("nodes").child(self.hostname).child("ram").set(system_ram, self.idToken)
        self.db.child("jobs").child("nodes").child(self.hostname).child("video").set(gpu_info.Name, self.idToken)
        print('OS Name: {0}'.format(os_name))
        print('OS Version: {0}'.format(os_version))
        print('CPU: {0}'.format(proc_info.Name))
        print('RAM: {0} GB'.format(system_ram))
        print('Graphics Card: {0}'.format(gpu_info.Name))
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
        self.log("Announcing availability for job is %s" % (available if 'available' else 'busy'), job)
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
        if job["name"] not in self.localjobqueue:
            self.job(job, True)
            self.localjobqueue.append(job["name"])

    def is_queued(self, job):
        return job["name"] in self.localjobqueue

    def handle_work(self, job):
        self.log("Handling work for job", job)
        if not self.is_queued(job):
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
        self.get_queue_node(job).child("state").set(state, self.idToken)

    def processing(self, job):
        self.queue(job)

        if not self.busy:
            self.db.child("jobs").child("nodes").child(self.hostname).child("current-job").set(job["name"], self.idToken)
            self.log ("Processing for job has begun.", job)
            job = self.next_job()
            if job is not None:
                self.begin_job(job)
            else:
                self.log ("No jobs left to process.")
        else:
            self.log("System is currently processing %s." % self.active_job["name"], job)

    def begin_job(self, job):
        self.inhibitor.inhibit()
        self.busy = True
        self.announce(job, False)
        self.announce_processing(job, True)
        self.on_begin_job(job)

    def update_job(self, job):
        self.get_queue_node(job).set(job, self.idToken)

    def job_complete(self, job, status="complete"):
        self.inhibitor.uninhibit()
        self.busy = False
        job["state"] = status
        self.get_queue_node(job).set(job, self.idToken)
        self.dequeue(job)

        self.get_avail_node(job).remove(self.idToken)
        self.db.child("jobs").child("nodes").child(self.hostname).child("current-job").set("", self.idToken)
        self.db.child("jobs").child("nodes").child(self.hostname).child("last-job").set(job['name'], self.idToken)

        self.log("Job complete", job)
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
            if "name" not in job:
                job["name"] = job_name
                self.db.child("jobs").child("queue").child(job_name).child("name").set(job_name, self.idToken)

            self.log ("Received job", job)
            self.handle_work(job)


def simulate_complete(jobqueue, job):
    job["url"] = "http://www.google.com"
    jobqueue.job_complete(job)

if __name__ == '__main__':
    jobqueue = FirebaseJobQueue()
    jobqueue.on_begin_job = lambda job: simulate_complete(jobqueue, job)

    jobqueue.monitor_jobs()
    while True:
        pass
