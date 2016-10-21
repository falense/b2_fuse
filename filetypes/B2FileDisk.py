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


class B2FileDisk(object):
    def __init__(self, b2fuse, path):
        self.b2fuse = b2fuse

        self.temp_filename = os.path.join(self.b2fuse.temp_folder, path)
        os.makedirs(self.temp_filename)

        self.temp_file = open(self.temp_filename, "wr+b")
        data = self.b2fuse.bucket.get_file(path)

        self.temp_file.write(data)

    def __getitem__(self, key):
        if isinstance(key, slice):
            self.temp_file.seek(key.start)
            return array.array('c',self.temp_file.read(key.stop-key.start))

        self.temp_file.seek(key)
        return array.array('c',self.temp_file.read(1))

    #def __getslice__(self, i, j):
    #    return self.__getitem__(slice(i, j))

    def __len__(self):
        return os.path.getsize(self.temp_filename)


    def delete(self):
        os.remove(self.temp_filename)

    def __del__(self):
        self.delete()

