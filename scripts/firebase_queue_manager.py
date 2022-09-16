import pyrebase
from firebasedata import LiveData
from firebase_config import config
from firebase_config import host_config
# Requirements
# collections
# pyrebase
# urllib3

class FirebaseQueueManager:
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


    # Write data
    # data = {"prompt": "Gandalf the Grey riding a horse or two"}
    # db.child("jobs").push(data)

    def announce(self, busy):
        print("write availability")
        self.db.child("jobs").child("available").child(self.hostname).set(busy, self.idToken)

    def handle_work(self, data):
        if 'worker' in data and data['worker'] == self.hostname:
            print("I have a job!")
            self.announce(False)
            return True
        return False


    def on_jobs_changed(self, response):
        data = response['data']
        path = response['path']
        print(data)

        if data is None or not self.handle_work(data):
            for job_name in data:
                job = data[job_name]
                print (job)
                self.handle_work(job)

if __name__ == '__main__':
    jobqueue = FirebaseQueueManager()
    jobqueue.monitor_jobs()
    while True:
        pass