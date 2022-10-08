import traceback
from threading import Thread

from firebase_job_queue import FirebaseJobQueue
import time
import os
from frontend.job_manager import JobInfo, UpdateCallbacks
from webui import txt2img
from firebase_config import uploader

from firebase_job_util import log, FirebaseUpdateEvent
from firebase_job_queue import dbref

class FirebaseRemoteSDService(FirebaseJobQueue):
    def __init__(self, tex2img):
        super().__init__()
        self.tex2img = tex2img
        
    def start(self):
        print (f"** SERVICE PID: {os.getpid()} **")
        self.monitor_jobs()

    def save_mem_image(self, job, image, file=None, index=None):
        if file is None:
            if index is None:
                file = "%s-%s-%d.png" % (job["name"], time.time(), index)
            else:
                file = "%s-%s.png" % (job["name"], time.time())

        log("Saving image %s" % file)
        image.save(file, format="PNG")
        images = self.get(self.data_node(job).child("images"))
        print(images)

        if images is None:
            images = [uploader.upload_file(file)]
            self.set(self.data_node(job).child("images"), images)
        else:
            images.append(uploader.upload_file(file))
            self.set(self.data_node(job).child("images"), images)

        os.remove(file)

    def save_grid(self, job, info):
        if "grid_file" in info and info['grid_file']:
            grid_file = info['grid_file']
            grid_path = os.path.join(info['outpath'], grid_file)
            grid = uploader.upload_file(grid_path, grid_file)
            self.set(self.data_node(job).child("grid"), grid)

    def on_job_canceled(self, job):
        if job != self.active_job:
            return
        log("Job canceled", job)
        self.complete_job(job, "canceled")
        self.callbacks.cancelled = True

    def on_job_started(self, job):
        self.active_thread = Thread(target=self.run_job, args=(job,))
        self.active_thread.start()

    def run_job(self, job):
        self.active_job = job
        self.set(dbref.child("jobs").child('nodes').child(self.hostname).child('current-job'), job)
        data = self.get(self.data_node(job))
        print(data)
        if "type" not in data:
            log("Job has no type", data)
            self.complete_job(job, "error: no type")
            return

        type = data["type"]

        if type == "txt2img":
            try:
                parameters = data["parameters"]
                prompt = parameters["prompt"]
                log("Beginning job txt2img job %s" % prompt)
                width = int(parameters["width"]) if "width" in parameters else 512
                height = int(parameters["height"]) if "height" in parameters else 512
                ddim_steps = int(parameters["ddim_steps"]) if "ddim_steps" in parameters else 50
                sampler_name = parameters["sampler_name"] if "sampler_name" in parameters else "k_lms"
                toggles = parameters["toggles"] if "toggles" in parameters else [1, 2, 3, 8, 9]
                realesrgan_model_name = parameters["realesrgan_model_name"] if "realesrgan_model_name" in parameters else "RealESRGAN"
                ddim_eta = float(parameters["ddim_eta"]) if "ddim_eta" in parameters else 0.0
                n_iter = int(parameters["n_iter"]) if "n_iter" in parameters else 1
                batch_size = int(parameters["batch_size"]) if "batch_size" in parameters else 1
                cfg_scale = float(parameters["cfg_scale"]) if "cfg_scale" in parameters else 7.5
                seed = parameters["seed"] if "seed" in parameters else ''
                fp = parameters["fp"] if "fp" in parameters else None
                variant_amount = float(parameters["variant_amount"]) if "variant_amount" in parameters else 0.0
                variant_seed = parameters["variant_seed"] if "variant_seed" in parameters else ''

                self.callbacks = UpdateCallbacks()

                self.callbacks.image_added = lambda file, i, image: self.save_mem_image(job, image, file + ".png", i)

                print("Generating prompt: " + prompt)

                data["seed"] = seed
                data["images"] = []
                (output_images, seed, info, stats) = txt2img(prompt,\
                        ddim_steps=ddim_steps,\
                        sampler_name=sampler_name, \
                        toggles=toggles, \
                        realesrgan_model_name=realesrgan_model_name,\
                        ddim_eta=ddim_eta, \
                        n_iter=n_iter, \
                        batch_size=batch_size, \
                        cfg_scale=cfg_scale, \
                        seed=seed,\
                        height=height, \
                        width=width, \
                        fp=fp, \
                        variant_amount=variant_amount, \
                        variant_seed=variant_seed,\
                         update_callbacks=self.callbacks)

                self.save_grid(job, info)
                print(f"Finished job {job}")
                self.set(dbref.child("jobs").child('nodes').child(self.hostname).child('last-job'), job)
                self.set(dbref.child("jobs").child('nodes').child(self.hostname).child('current-job'), "")
                self.complete_job(job)
            except Exception as error:
                log(f"Error generating image {error}", job)
                traceback.print_exc()
                self.complete_job(job, "error: %s" % error)
            return

        log("Job type %s is not recognized." % type, job)
        self.complete_job(job, "error: unknown type")




