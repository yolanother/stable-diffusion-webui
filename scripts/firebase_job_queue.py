import pyrebase
import time
from firebase_config import config
from firebase_config import host_config
# Requirements
# collections
# pyrebase
# urllib3

class FirebaseJobQueue:
    busy = False
    localjobqueue = []
    on_begin_job = None

    def __init__(self):
        self.config = config
        self.hostname = host_config['hostname']

    def monitor_jobs(self):
        self.firebase = pyrebase.initialize_app(config)
        auth = self.firebase.auth().sign_in_with_email_and_password("firebasejobqueue@doubtech.com", config["apiKey"])
        self.idToken = auth['idToken']
        print (auth)
        print ("Logged in.")
        self.db = self.firebase.database()
        self.db.child("jobs").child("queue").stream(self.on_jobs_changed)
        self.ping()

    def ping(self):
        self.db.child("jobs").child("nodes").child(self.hostname).set(time.time(), self.idToken)



    # Write data
    # data = {"prompt": "Gandalf the Grey riding a horse or two"}
    # db.child("jobs").push(data)

    def get_avail_node(self, job):
        return self.db.child("jobs").child("available").child(job["name"]).child(self.hostname)

    def get_queue_node(self, job):
        return self.db.child("jobs").child("queue").child(job["name"])

    def announce(self, job, busy):
        print("write availability for job %s" % job)
        self.get_avail_node(job).set(busy, self.idToken)

    def announce_processing(self, job, busy):
        print("write availability for job %s" % job)
        self.update_state(job, "processing")

    def handle_work(self, job):
        self.ping()
        if 'state' in job and job['state'] == "complete":
            return True
        elif not self.busy:
            self.announce(job, True)
        if 'worker' in job and job['worker'] == self.hostname:
            self.processing(job)
            return True
        return False

    def update_state(self, job, state):
        self.get_queue_node(job).child("state").set(state, self.idToken)

    def processing(self, job):
        if job not in self.localjobqueue:
            self.localjobqueue.append(job)

        if not self.busy:
            print ("Processing job %s" % job)
            job = self.localjobqueue[0]
            self.busy = True
            self.announce(job, False)
            self.announce_processing(job, True)
            self.on_begin_job(job)
        else:
            print ("System busy, queuing job %s" % job)

    def job_complete(self, job):
        self.busy = False
        job["state"] = "complete"
        self.get_queue_node(job).set(job, self.idToken)
        if job in self.localjobqueue:
            self.localjobqueue.remove(job)
        self.get_avail_node(job).remove(self.idToken)

        if len(self.localjobqueue) > 0:
            self.processing(self.localjobqueue[0])

    def on_jobs_changed(self, response):
        data = response['data']
        path = [x for x in response['path'].split('/') if x]
        if data is None:
            return
        print (path)
        print(data)

        if len(path) == 0:
            for job_name in data:
                print ("Updating state for %s" % job_name)
                job = data[job_name]
                job["name"] = job_name
                print (job)
                self.handle_work(job)
        elif path is not None and path != "/":
            job_name = path[0]
            job = self.db.child("jobs").child("queue").child(job_name).get().val()
            job["name"] = job_name
            print ("Data has been updated for job %s" % path[0])
            print(job)
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
