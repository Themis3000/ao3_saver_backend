from abc import ABC, abstractmethod


class StorageManager(ABC):
    @property
    @abstractmethod
    def storage_type(self):
        pass

    @abstractmethod
    def store_file(self, key: str, data: bytes) -> None:
        pass

    @abstractmethod
    def delete_file(self, key: str) -> None:
        pass

    @abstractmethod
    def get_file(self, key: str) -> bytes:
        pass

    def store_work(self, work_id: int, work: bytes, uploaded_time: int, retrieved_from: str, file_format: str) -> None:
        pass

