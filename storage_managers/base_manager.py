import hashlib
import html
from abc import ABC, abstractmethod
from typing import List
from db import (get_head_work_storage_data, add_storage_entry, update_storage_patch,
                get_storage_entry, WorkNotFound, SupportingObject, object_exists, create_object_entry,
                create_object_index_entry, find_object_index_entry, SupportingCachedObject, StorageData)
import uuid
import bsdiff4
import zlib
from bs4.dammit import UnicodeDammit


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
                   file_format: str, supporting_objects: List[SupportingObject | SupportingCachedObject],
                   title: str = None, author: str = None) -> None:
        if supporting_objects:
            if file_format != 'html':
                raise NotImplemented("Cannot handle supporting objects with non-html files.")
            work = self.rewrite_html_sources(work, supporting_objects, work_id)

        previous_head_work = get_head_work_storage_data(work_id, file_format)
        work_sha1 = hashlib.sha1(work).hexdigest()
        if previous_head_work is not None and previous_head_work.sha1 == work_sha1:
            raise DuplicateDetected("The work being stored was found to be a duplicate.")
        storage_key = f"{work_id}_{work_sha1}"
        self.store_file_compressed(storage_key, work)
        storage_id = add_storage_entry(work_id, uploaded_time, updated_time, storage_key, retrieved_from, file_format,
                                       work_sha1, title, author)

        if previous_head_work is not None:  # Create diff file to maintain history
            old_work = self.get_file_compressed(previous_head_work.location)
            diff = bsdiff4.diff(work, old_work)
            self.store_file_compressed(previous_head_work.location, diff)
            update_storage_patch(previous_head_work.storage_id, storage_id)

    def get_work_by_lookup(self, work_id: int, file_format: str) -> bytes | None:
        head_work = get_head_work_storage_data(work_id, file_format)
        if head_work is None:
            return None
        return self.get_file_compressed(head_work.location)

    def get_work(self, storage_id: int) -> tuple[bytes, StorageData]:
        storage_entry = get_storage_entry(storage_id)
        if storage_entry is None:
            raise WorkNotFound("The archived work doesn't seem to exist.")
        storage_patches = []  # list of storage entries to be fetched for patching
        original_storage_entry = storage_entry
        for _ in range(100):  # limiting iterations just in case
            storage_patches.insert(0, storage_entry)
            if storage_entry.patch_of is None:
                break
            storage_entry = get_storage_entry(storage_entry.patch_of)
        else:
            raise TooManyIterations("Too many iterations to reach head work. Is there an infinite loop?")

        master_file = self.get_file_compressed(storage_patches.pop(0).location)
        for storage_patch in storage_patches:
            diff_bytes = self.get_file_compressed(storage_patch.location)
            master_file = bsdiff4.patch(master_file, diff_bytes)

        return master_file, original_storage_entry

    def rewrite_html_sources(self, work: bytes, supporting_objects: List[SupportingObject | SupportingCachedObject],
                             work_id: int) -> bytes:
        work_text = UnicodeDammit(work, is_html=True).unicode_markup

        # Validate that all supporting object urls are actually present in work
        for supporting_object in supporting_objects:
            if supporting_object.url not in work_text and html.escape(supporting_object.url) not in work_text:
                raise ValueError(f"Supporting object URL '{supporting_object.url}' not found in work {work_id}")

        # Upload supporting objects, if not already uploaded.
        for supporting_object in supporting_objects:
            if isinstance(supporting_object, SupportingCachedObject):
                work_text = work_text.replace(supporting_object.url, f"/objects/{supporting_object.object_id}", 1)
                continue

            sha1 = supporting_object.data_sha1()
            object_index_id: int
            if not object_exists(sha1):
                file_key = f"obj_{sha1}"
                self.store_file(file_key, supporting_object.data)
                create_object_entry(sha1, file_key)
                object_index_id = create_object_index_entry(sha1, supporting_object.url, supporting_object.etag,
                                                            work_id, supporting_object.mimetype)
            else:
                object_index_id = find_object_index_entry(sha1, supporting_object.url, supporting_object.etag, work_id)
                if object_index_id is None:
                    object_index_id = create_object_index_entry(sha1, supporting_object.url, supporting_object.etag,
                                                                work_id, supporting_object.mimetype)
            if supporting_object.url in work_text:
                work_text = work_text.replace(supporting_object.url, f"/objects/{object_index_id}", 1)
            else:
                escaped_url = html.escape(supporting_object.url)
                if escaped_url in work_text:
                    work_text = work_text.replace(escaped_url, f"/objects/{object_index_id}", 1)
                else:
                    raise ValueError(f"Supporting object URL '{supporting_object.url}' not found despite passing check in {work_id}")

        return work_text.encode('utf-8')


class TooManyIterations(Exception):
    pass


class DuplicateDetected(Exception):
    pass
