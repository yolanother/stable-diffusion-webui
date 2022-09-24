import json
import os
import traceback
from threading import Thread, Lock
from abc import abstractmethod

import pyrebase
import time
from firebase_config import config
from firebase_config import host_config
from firebase_job_util import log, FirebaseUpdateEvent

firebase = pyrebase.initialize_app(config)
auth = firebase.auth().sign_in_with_email_and_password("firebasejobqueue@doubtech.com", config["apiKey"])
idToken = auth['idToken']
dbref = firebase.database()

class FirebaseJobQueue:
    def __init__(self):
        self.busy = False
        self.hostname = host_config['hostname']
        self.jobqueue = []
        self.lock = Lock()
        self.active_job = None

    def data_root_node(self, job):
        return dbref.child("jobs").child("data").child(job)
    def data_job_node(self, job):
        return self.data_root_node(job).child("job")

    def data_node(self, job):
        return self.data_root_node(job).child("data")

    def queue_root(self):
        return dbref.child("jobs").child("queue")

    def queue_node(self, job):
        return self.queue_root().child(job)

    def update_timestamp(self, job):
        self.set(self.queue_node(job).child("timestamp"), time.time())
        self.set(self.data_job_node(job).child("timestamp"), time.time())

    def set(self, node, data):
        try:
            print("Setting node: " + node.path)
            node.set(data, idToken)
        except Exception as e:
            print(f"Error setting node: {node.path}: {e}")
            traceback.print_exc()

    def update(self, node, data):
        try:
            node.update(data, idToken)
        except Exception as e:
            print(f"Error setting node: {node.path}: {e}")
            traceback.print_exc()

    def get(self, node):
        try:
            print("Getting node: " + node.path)
            return node.get(idToken).val()
        except Exception as e:
            print(f"Error setting node: {node.path}: {e}")
            traceback.print_exc()

    def monitor_jobs(self):
        print ('Monitoring jobs...')
        self.queue_root().stream(self.queue_update, idToken)

    def queue_update(self, status):
        print(status)
        ev = FirebaseUpdateEvent(status)
        job = ev.segments[0]
        log(ev)

        if ev.data is None:
            return

        if '/' == ev.path:
            for job in ev.data.keys():
                self.handle_job(job, ev.data[job])
        if ev.segments[-1] == 'worker' and ev.data == self.hostname:
            if job not in self.jobqueue:
                log(f"Received job assignment for {job}.")
                self.jobqueue.append(job)
                if not self.busy:
                    log(f"Not busy, processing job {job}.")
                    self.process_job(job)
                else:
                    log(f"Busy, adding job {job} to queue.")
        elif ev.segments[-1] == 'status':
            self.handle_status(job, ev.data)
        elif len(ev.segments) == 1:
            self.handle_job(ev.segments[-1], ev.data)

    def handle_job(self, job, data):
        log(f"Handling job: {job} => {data}")

        if 'status' in data:
            self.handle_status(job, data['status'])
        else:
            print(job)
            for key in data.keys():
                self.handle_job(key, data[key])

    def handle_status(self, job, status):
        log(f"Received status update: {status} for {job}")
        if status == 'requesting':
            self.process_request(job)
        if status == 'canceled':
            self.handle_cancel(job)
        if status == 'complete':
            if job == self.active_job:
                self.complete_job(job)
            elif job in self.jobqueue:
                self.cleanup_job(job)

    def update_state(self, job, state):
        self.queue_node(job).update({u'status': state}, idToken)
        self.data_job_node(job).update({u'status': state}, idToken)
        self.data_job_node(job).update({u'timestamp': time.time()}, idToken)

    def update_availability(self, job):
        self.queue_node(job).child("available-nodes").child(self.hostname).set(not self.busy, idToken)

    def process_request(self, job):

        if job is None:
            return

        if not self.busy:
            log(f"Received request. Announcing availability for job {job}.")
            self.update_availability(job)

    def process_job(self, job):
        try:
            self.active_job = job
            self.lock.acquire()
            self.busy = True
            self.update_state(job, 'processing')
            self.lock.release()
            self.on_job_started(job)
        except Exception as e:
            log(f"Error processing job {job}: {e}")
            traceback.print_exc()
            self.complete_job(job, f'error: {e}')

    @abstractmethod
    def on_job_started(self, job):
        pass

    def cleanup_job(self, job):
        if job in self.jobqueue:
            self.jobqueue.remove(job)

    def next_in_queue(self):
        if len(self.jobqueue) > 0:
            self.process_job(self.jobqueue[0])
        else:
            queued = self.get(self.queue_root())
            if queued is not None:
                for job in queued.keys():
                    if job not in self.jobqueue:
                        self.update_availability(job)

    def cancel(self, job):
        self.complete_job(job, 'canceled')

    def complete_job(self, job, status='complete'):
        log(f"Job {job} completed with status {status}.")
        if job == self.active_job:
            self.update_timestamp(job)
            self.active_job = None
            self.lock.acquire()
            self.busy = False
            self.update_state(job, status)
            self.lock.release()
            self.cleanup_job(job)
            self.on_job_completed(job)
            self.next_in_queue()
        else:
            self.cleanup_job(job)

    def fail_job(self, job):
        self.complete_job(job, 'failed')

    def handle_cancel(self, job):
        self.update_state(job, 'canceled')
        self.on_job_canceled(job)
        self.cleanup_job(job)
        self.next_in_queue()

    @abstractmethod
    def on_job_canceled(self, job):
        pass

    @abstractmethod
    def on_job_completed(self, job):
        pass


class FirebaseQueueSimulator(FirebaseJobQueue):
    def __init__(self):
        super().__init__()

    def on_job_started(self, job):
        print ('Processing job: %s' % job)
        self.data_node(job).child('extra-data').set({'foo': 'bar'}, idToken)

    def on_job_completed(self, job):
        print ('Job complete: %s' % job)

    def on_job_canceled(self, job):
        print ('Job canceled: %s' % job)
        self.cancel(job)

if __name__ == '__main__':
    queue = FirebaseQueueSimulator()
    queue.monitor_jobs()
    while True:
        pass