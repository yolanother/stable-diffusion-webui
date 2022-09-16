from firebase_job_queue import FirebaseJobQueue
import webui

class FirebaseRemoteSDService(FirebaseJobQueue):
    def __init__(self, webui):
        super().__init__()
        self.job_queue = FirebaseJobQueue()
        self.job_queue.on_begin_job = self.on_begin_job
        
    def start(self):
        self.job_queue.monitor_jobs()

    def on_begin_job(self, job):
        print("on_begin_job")
        if job["type"] == "text":
            prompt = job["prompt"]
            width = job["width"] if "width" in job else 512
            height = job["height"] if "height" in job else 512
            ddim_steps = job["ddim_steps"] if "ddim_steps" in job else 50
            sampler_name = job["sampler_name"] if "sampler_name" in job else "k_lms"
            toggles = job["toggles"] if "toggles" in job else [1, 2, 3]
            realesrgan_model_name = job["realesrgan_model_name"] if "realesrgan_model_name" in job else "RealESRGAN"
            ddim_eta = job["ddim_eta"] if "ddim_eta" in job else 0.0
            n_iter = job["n_iter"] if "n_iter" in job else 1
            batch_size = job["batch_size"] if "batch_size" in job else 1
            cfg_scale = job["cfg_scale"] if "cfg_scale" in job else 7.5
            seed = job["seed"] if "seed" in job else ''
            fp = job["fp"] if "fp" in job else None
            variant_amount = job["variant_amount"] if "variant_amount" in job else 0.0
            variant_seed = job["variant_seed"] if "variant_seed" in job else ''

            webui.txt2img(prompt,\
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