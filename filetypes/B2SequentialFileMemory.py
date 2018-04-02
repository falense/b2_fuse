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

from b2.download_dest import DownloadDestBytes

from B2BaseFile import B2BaseFile


class B2SequentialFileMemory(B2BaseFile):
    def __init__(self, b2fuse, file_info, new_file=False):
        super(B2SequentialFileMemory, self).__init__(b2fuse, file_info)
        
        self._dirty = False
        if new_file:
            self.data = array.array('c')
            self._dirty = True
        else:
            download_dest = DownloadDestBytes()
            self.b2fuse.bucket_api.download_file_by_id(self.file_info['fileId'], download_dest)
            self.data = array.array('c', download_dest.get_bytes_written())

    # def __getitem__(self, key):
    #    if isinstance(key, slice):
    #        return self.data[key.start:key.stop] 
    #    return self.data[key]

    def upload(self):
        if self._dirty:
            self.b2fuse.bucket_api.upload_bytes(bytes(self.data.tostring()), self.file_info['fileName'])
            self.b2fuse._update_directory_structure()
            self.file_info = self.b2fuse._directories.get_file_info(self.file_info['fileName'])

        self._dirty = False

    #def __setitem__(self, key, value):
    #    self.data[key] = value

    def __len__(self):
        return len(self.data)

    #def __del__(self):
    #    self.delete()

    def write(self, offset, data):
        if offset == len(self):
            self.data.extend(data)
        elif offset+len(data) < len(self):
            for i in range(len(data)):
                self.data[offset+i] = data[i]
        else:
			extend_length = offset-len(data)
			self.data.extend([0 for i in range(extend_length)])
			self.write(offset, data)

    def read(self, offset, length):
        return self.data[offset:offset + length].tostring()

    def truncate(self, length):
        self.data = self.data[:length]

    def set_dirty(self, new_value):
        self._dirty = new_value

    def delete(self, delete_online):
        if delete_online:
            self.b2fuse.bucket_api.delete_file_version(
                self.file_info['fileId'], self.file_info['fileName']
            )
        del self.data
