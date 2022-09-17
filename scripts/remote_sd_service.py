from firebase_job_queue import FirebaseJobQueue
import time
import os
import pyrebase
from firebase_config import config
from firebase_config import host_config

class FirebaseRemoteSDService(FirebaseJobQueue):

    def __init__(self, tex2img):
        super().__init__()
        self.job_queue = FirebaseJobQueue()
        self.job_queue.on_begin_job = self.on_begin_job
        self.tex2img = tex2img
        
    def start(self):
        self.job_queue.monitor_jobs()

    def save_image(self, job, file, name=None):
        self.job_queue.log("Uploading image %s" % file)
        storage = self.job_queue.firebase.storage()
        if name is None:
            name = file
        storage.child(job["type"]).child(name).put(file, self.job_queue.idToken)
        url = storage.child(job["type"]).child(name).get_url(None)
        print ("Generated url %s" % url)
        return url

    def save_images(self, job, images, info):
        count = 1
        imageset = []
        for image in images:
            file = "%s-%s-%d.png" % (job["name"], time.time(), count)
            image.save(file, format="PNG")
            self.job_queue.log("Saving image %s" % file)
            imageset.append(self.save_image(job, file))
            os.remove(file)
            count += 1

        if "grid_file" in info and info['grid_file'] is not '':
            grid_file = info['grid_file']
            grid_path = os.path.join(info['outpath'], grid_file)
            job["grid"] = self.save_image(job, grid_path, grid_file)

        job["images"] = imageset
        return job

    def on_begin_job(self, job):
        if "type" not in job:
            self.job_queue.log("Job has no type", job)
            self.job_queue.job_complete(job)
            return

        type = job["type"]

        if type == "txt2img":
            self.job_queue.log("Beginning job txt2img job %s" % job["name"])
            parameters = job["parameters"]
            prompt = parameters["prompt"]
            width = int(parameters["width"]) if "width" in parameters else 512
            height = int(parameters["height"]) if "height" in parameters else 512
            ddim_steps = int(parameters["ddim_steps"]) if "ddim_steps" in parameters else 50
            sampler_name = parameters["sampler_name"] if "sampler_name" in parameters else "k_lms"
            toggles = parameters["toggles"] if "toggles" in parameters else [1, 2, 3]
            realesrgan_model_name = parameters["realesrgan_model_name"] if "realesrgan_model_name" in parameters else "RealESRGAN"
            ddim_eta = float(parameters["ddim_eta"]) if "ddim_eta" in parameters else 0.0
            n_iter = int(parameters["n_iter"]) if "n_iter" in parameters else 1
            batch_size = int(parameters["batch_size"]) if "batch_size" in parameters else 1
            cfg_scale = float(parameters["cfg_scale"]) if "cfg_scale" in parameters else 7.5
            seed = parameters["seed"] if "seed" in parameters else ''
            fp = parameters["fp"] if "fp" in parameters else None
            variant_amount = float(parameters["variant_amount"]) if "variant_amount" in parameters else 0.0
            variant_seed = parameters["variant_seed"] if "variant_seed" in parameters else ''

            print("Generating prompt: " + prompt)

            try:
                (output_images, seed, info, stats) = self.tex2img(prompt,\
                        ddim_steps,\
                        sampler_name, \
                        toggles, \
                        realesrgan_model_name,\
                        ddim_eta, \
                        n_iter, \
                        batch_size, \
                        cfg_scale, \
                        seed,\
                        height, \
                        width, \
                        fp, \
                        variant_amount, \
                        variant_seed)

                job["seed"] = seed
                job = self.save_images(job, output_images, info)
                print ("Finished job %s %s" % (job["name"], job))
            except:
                self.job_queue.log("Error generating image", job)
                return
            self.job_queue.job_complete(job=job)
            return

        self.job_queue.log("Job type %s is not recognized." % type, job)
        self.job_queue.job_complete(job)




