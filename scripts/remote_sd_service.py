from firebase_job_queue import FirebaseJobQueue
import time
import os
from frontend.job_manager import JobInfo, UpdateCallbacks
from webui import txt2img
from firebase_config import uploader

class FirebaseRemoteSDService(FirebaseJobQueue):

    def __init__(self, tex2img):
        super().__init__()
        self.job_queue = FirebaseJobQueue()
        self.job_queue.on_begin_job = self.on_begin_job
        self.tex2img = tex2img
        
    def start(self):
        print (f"** SERVICE PID: {os.getpid()} **")
        self.job_queue.monitor_jobs()

    def save_mem_image(self, job, image, file=None, index=None):
        if file is None:
            if index is None:
                file = "%s-%s-%d.png" % (job["name"], time.time(), index)
            else:
                file = "%s-%s.png" % (job["name"], time.time())

        self.job_queue.log("Saving image %s" % file)
        image.save(file, format="PNG")
        image_set = job["images"]
        if image_set is None:
            image_set = []
        print ("AARON: Using uploader %s" % uploader)
        image_set.append(uploader.upload_file(file))
        job["images"] = image_set
        self.job_queue.update_job(job)

        os.remove(file)

    def save_images(self, job, images, info):
        count = 1
        imageset = []
        #for image in images:
        #    self.save_mem_image(job, image, None, count)
        #    count += 1

        if "grid_file" in info and info['grid_file'] is not '':
            grid_file = info['grid_file']
            grid_path = os.path.join(info['outpath'], grid_file)
            job["grid"] = uploader.upload_file(grid_path, grid_file)
            job['timestamp'] = time.time()

        #job["images"] = imageset
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

            callbacks = UpdateCallbacks()

            callbacks.image_added = lambda file, i, image: self.save_mem_image(job, image, file + ".png", i)

            print("Generating prompt: " + prompt)

            job["seed"] = seed
            job["images"] = []
            try:
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
                         update_callbacks=callbacks)

                job = self.save_images(job, output_images, info)
                print ("Finished job %s %s" % (job["name"], job))
            except Exception as error:
                self.job_queue.log(f"Error generating image {error}", job)
                return
            self.job_queue.job_complete(job=job)
            return

        self.job_queue.log("Job type %s is not recognized." % type, job)
        self.job_queue.job_complete(job)

        print ('================= Job complete =================')
        for r in range(0, 100):
            print('                                       ')




