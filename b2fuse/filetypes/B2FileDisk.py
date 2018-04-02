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
import os
import os.path

from b2.download_dest import DownloadDestBytes

from B2BaseFile import B2BaseFile

class B2FileDisk(B2BaseFile):
    def __init__(self, b2fuse, file_info, new_file=False):
        super(B2FileDisk, self).__init__(b2fuse, file_info)
        
        self.temp_filename = os.path.join(self.b2fuse.temp_folder, self.file_info['fileName'])
        
        folder = os.path.join(self.b2fuse.temp_folder, os.path.dirname(self.file_info['fileName']))
        if not os.path.exists(folder):
            os.makedirs(folder)

        self._dirty = False
        
        if os.path.exists(self.temp_filename):
            os.remove(self.temp_filename)
        
        self.temp_file = open(self.temp_filename, "wr+b")
    
        if new_file:
            self._dirty = True
        else:
            download_dest = DownloadDestBytes()
            self.b2fuse.bucket_api.download_file_by_id(self.file_info['fileId'], download_dest)
            self.temp_file.write(download_dest.get_bytes_written())
            
        
    def __len__(self):
        return os.path.getsize(self.temp_filename)

    def delete(self, delete_online):
        if delete_online:
            self.b2fuse.bucket_api.delete_file_version(
                self.file_info['fileId'], self.file_info['fileName']
            )
        self.temp_file.close()
        os.remove(self.temp_filename)

    #def __del__(self):
    #    self.delete()
        
    def upload(self):
        if self._dirty:
            data = self.read(0,len(self))
            self.b2fuse.bucket_api.upload_bytes(bytes(data), self.file_info['fileName'])
            self.b2fuse._update_directory_structure()
            self.file_info = self.b2fuse._directories.get_file_info(self.file_info['fileName'])

        self._dirty = False

    def write(self, offset, data):
        self.temp_file.seek(offset)
        self.temp_file.write(data)
        self.temp_file.flush()

    def read(self, offset, length):
        self.temp_file.seek(offset)
        return self.temp_file.read(length)

    def truncate(self, length):
        self.temp_file.seek(0)
        self.temp_file.truncate(length)

    def set_dirty(self, new_value):
        self._dirty = new_value

