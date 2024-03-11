from abc import ABC, abstractmethod
from db import get_head_work_storage_data, add_storage_entry, add_work_entry, update_storage_patch
import uuid
import bsdiff4


class StorageManager(ABC):
    @abstractmethod
    def store_file(self, key: str, data: bytes) -> None:
        pass

    @abstractmethod
    def delete_file(self, key: str) -> None:
        pass

    @abstractmethod
    def get_file(self, key: str) -> bytes:
        pass

    def store_work(self, work_id: int, work: bytes, uploaded_time: int, updated_time: int, retrieved_from: str,
                   file_format: str, work_title=None) -> None:
        storage_key = f"{work_id}_{uuid.uuid4()}"
        self.store_file(storage_key, work)
        storage_id = add_storage_entry(work_id, uploaded_time, updated_time, storage_key, retrieved_from, file_format)

        head_work = get_head_work_storage_data(work_id, file_format)
        if head_work is not None:  # Create diff file to maintain history
            old_work = self.get_file(head_work.location)
            diff = bsdiff4.diff(work, old_work)
            self.store_file(head_work.location, diff)
            update_storage_patch(head_work.storage_id, storage_id)
        else:  # Create record of work existing in db
            add_work_entry(work_id, True)

    def get_work(self, work_id: int, file_format: str) -> bytes:
        head_work = get_head_work_storage_data(work_id, file_format)
        return self.get_file(head_work.location)
