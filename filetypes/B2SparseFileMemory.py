#!/usr/bin/env python
# -*- coding: utf-8 -*-

#The MIT License (MIT)

#Copyright (c) 2015 Sondre Engebraaten

#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import array

import logging

from B2BaseFile import B2BaseFile


class B2SparseFileMemory(B2BaseFile):
    def __init__(self, b2fuse, path, new_file=False):
        super(B2SparseFileMemory, self).__init__(b2fuse, path)
        self.logger = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        self.prefect_parts = 1

        self._dirty = False

        self.part_size = 1024**2
        self.upload_part_size = 100 * 1024**2
        if new_file:
            self.data = [array.array('c')]
            self._dirty = True

            self.file_parts = [True]
            self.size = 0
        else:
            self.size = self.b2fuse.bucket.get_file_info(path)['size']
            num_file_parts = 1 + self.size / self.part_size
            self.file_parts = [False] * num_file_parts
            self.ready_parts = [False] * num_file_parts
            self.data = [None] * num_file_parts

    def upload(self):
        if self._dirty:
            self.b2fuse.bucket.put_file(self.path, self.read(0, len(self)))

        self._dirty = False

    def __len__(self):
        return self.size

    def write(self, offset, data):
        if offset == len(self):
            part_index = len(self.data) - 1

            length = len(data)

            if length < self.part_size - len(self.data[part_index]):
                self.data[part_index].extend(data)

                self.size += length
            else:
                available_bytes = self.part_size - len(self.data[part_index])
                self.data[part_index].extend(data[:available_bytes])

                leftover_data = data[available_bytes:]
                self.data.append(array.array('c', leftover_data))
                self.file_parts.append(True)

                self.size += length
        else:
            raise NotImplemented("Random write")

    def _data_available(self, start_index, end_index):
        start_part = int(start_index) / self.part_size
        end_part = int(end_index) / self.part_size

        for part in range(start_part, end_part + 1, 1):
            if not self.ready_parts[part]:
                return False

        return True

    def _fetch_parts(self, start_index, end_index, prefetch=False):
        start_part = int(start_index) / self.part_size
        end_part = int(end_index) / self.part_size

        if end_part + self.prefect_parts < len(self.data):
            end_part += self.prefect_parts

        for part in range(start_part, end_part + 1, 1):
            if not self.file_parts[part]:
                i_start = part * self.part_size
                i_end = (part + 1) * self.part_size - 1

                if prefetch:
                    self.logger.warning("Prefetching %s %s" % (start_part, end_part))

                    def callback(byte_range, data):
                        self.data[part] = array.array("c", data)
                        self.ready_parts[part] = True

                    self.b2fuse.bucket.get_file_callback(
                        self.path, byte_range=(i_start, i_end), callback=callback
                    )
                else:
                    data = self.b2fuse.bucket.get_file(self.path, byte_range=(i_start, i_end))
                    self.data[part] = array.array("c", data)
                    self.ready_parts[part] = True

            while not prefetch and not self.ready_parts[part]:
                from time import sleep

                sleep(0.1)

            self.file_parts[part] = True

    def read(self, offset, length):
        if offset + length > len(self):
            length = len(self) - offset

        if not self._data_available(offset, offset + length):
            self._fetch_parts(offset, offset + length)

        self._fetch_parts(offset, min(len(self), offset + length + 1024 * 1024), prefetch=True)

        start_part = int(offset) / self.part_size
        end_part = int(offset + length) / self.part_size

        chunk_start_index = offset % self.part_size

        temp_length = min(length, self.part_size)
        temp_data = self.data[start_part][chunk_start_index:chunk_start_index + temp_length]
        chunk = array.array('c', temp_data)

        if length > (self.part_size - chunk_start_index):
            for part in range(start_part + 1, end_part, 1):
                chunk.extend(self.data[part])

            chunk_end_index = (offset + length) % self.part_size
            if chunk_end_index != 0:
                chunk.extend(self.data[end_part][:chunk_end_index])

        return chunk

    def truncate(self, length):
        if length == 0:
            self.data = [array.array('c')]
            self._dirty = True

            self.file_parts = [True]
            self.size = 0
        else:
            raise NotImplemented("Truncate size other than 0")

    def set_dirty(self, new_value):
        self._dirty = new_value

    def delete(self):
        del self.data
