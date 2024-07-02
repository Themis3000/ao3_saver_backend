from abc import ABC, abstractmethod
from db import get_head_work_storage_data, add_storage_entry, add_work_entry, update_storage_patch,\
    get_work_storage_by_timestamp, get_storage_entry
import uuid
import bsdiff4
import zlib


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

    def store_file_compressed(self, key: str, data: bytes) -> None:
        self.store_file(key, zlib.compress(data))

    def get_file_compressed(self, key: str) -> bytes:
        return zlib.decompress(self.get_file(key))

    def store_work(self, work_id: int, work: bytes, uploaded_time: int, updated_time: int, retrieved_from: str,
                   file_format: str, work_title=None) -> None:
        storage_key = f"{work_id}_{uuid.uuid4()}"
        self.store_file_compressed(storage_key, work)
        storage_id = add_storage_entry(work_id, uploaded_time, updated_time, storage_key, retrieved_from, file_format)

        head_work = get_head_work_storage_data(work_id, file_format)
        if head_work is not None:  # Create diff file to maintain history
            old_work = self.get_file_compressed(head_work.location)
            diff = bsdiff4.diff(work, old_work)
            self.store_file_compressed(head_work.location, diff)
            update_storage_patch(head_work.storage_id, storage_id)
        else:  # Create record of work existing in db
            add_work_entry(work_id, True, work_title)

    def get_work(self, work_id: int, file_format: str) -> bytes:
        head_work = get_head_work_storage_data(work_id, file_format)
        return self.get_file_compressed(head_work.location)

    def get_archived_work(self, work_id: int, timestamp: int, file_format: str) -> bytes:
        work_entry = get_work_storage_by_timestamp(work_id, timestamp, file_format)
        storage_patches = []  # list of storage entries to be fetched for patching
        for _ in range(100):  # limiting iterations just in case
            storage_patches.insert(0, work_entry)
            if work_entry.patch_of is None:
                break
            work_entry = get_storage_entry(work_entry.patch_of)
        else:
            raise TooManyIterations("Too many iterations to reach head work. Is there an infinite loop?")

        master_file = self.get_file_compressed(storage_patches.pop().location)
        for storage_patch in storage_patches:
            # TODO:Themis this looked like a bug so I fixed it blindly. Was it a bug? If so, remove commented code.
            # diff_bytes = self.get_file_compressed(storage_patch.pop().location)
            diff_bytes = self.get_file_compressed(storage_patch.location)
            master_file = bsdiff4.patch(master_file, diff_bytes)

        return master_file


class TooManyIterations(Exception):
    pass
