from abc import ABC, abstractmethod


class Uploader(ABC):
    @abstractmethod
    def upload_file(self, file, object_name=None):
        pass

    @abstractmethod
    def upload_image(self, image, object_name):
        pass

    @abstractmethod
    def upload_buffer(self, buffer, object_name):
        pass