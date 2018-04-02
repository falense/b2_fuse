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


from B2BaseFile import B2BaseFile

class B2HashFile(B2BaseFile):
    def __init__(self, b2fuse, file_info, new_file=False):
        super(B2HashFile, self).__init__(b2fuse, file_info)

        file_hash = file_info['contentSha1'] + "\n"
        self.data = array.array('c', file_hash.encode("utf-8"))

    #def __getitem__(self, key):
    #    if isinstance(key, slice):
    #        return self.data[key.start:key.stop] 
    #    return self.data[key]

    #def __getslice__(self, i, j):
    #    return self.__getitem__(slice(i, j))

    def __len__(self):
        return len(self.data)

    def upload(self):
        return

    def write(self, offset, data):
        return

    def read(self, offset, length):
        return self.data.tostring()
