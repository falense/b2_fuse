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

import os
import errno
import logging
import shutil

from collections import defaultdict
from fuse import FuseOSError, Operations
from stat import S_IFDIR, S_IFREG
from time import time

from b2.account_info.in_memory import InMemoryAccountInfo
from b2.api import B2Api

from filetypes.B2SequentialFileMemory import B2SequentialFileMemory
from filetypes.B2FileDisk import B2FileDisk
from filetypes.B2HashFile import B2HashFile
from directory_structure import DirectoryStructure
from cached_bucket import CachedBucket



class B2Fuse(Operations):
    def __init__(
        self, account_id, application_key, bucket_id, enable_hashfiles, temp_folder,
        use_disk
    ):
        account_info = InMemoryAccountInfo()
        self.api = B2Api(account_info)
        self.api.authorize_account('production', account_id, application_key)
        self.bucket_api = CachedBucket(self.api, bucket_id)

        self.logger = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        self.enable_hashfiles = enable_hashfiles
        self.temp_folder = temp_folder
        self.use_disk = use_disk

        if self.use_disk:
            if os.path.exists(self.temp_folder):
                self.logger.error("Temporary folder exists, exiting")
                exit(1)
                
            os.makedirs(self.temp_folder)
            self.B2File = B2FileDisk
        else:
            self.B2File = B2SequentialFileMemory

        self._directories = DirectoryStructure()
        self.local_directories = []

        self.open_files = defaultdict(self.B2File)

        self.fd = 0

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder)
            
        return

    # Helper methods
    # ==================

    def _exists(self, path, include_hash=True):
        #Handle hash files
        if include_hash and path.endswith(".sha1"):
            path = path[:-5]

        #File is in bucket
        if self._directories.is_file(path):
            return True

        #File is open (but possibly not in bucket)
        if path in self.open_files.keys():
            return True

        return False

    def _get_memory_consumption(self):
        open_file_sizes = map(lambda f: len(f), self.open_files.values())

        memory = sum(open_file_sizes)

        return float(memory) / (1024 * 1024)

    def _get_cloud_space_consumption(self):

        directories = [self._directories._directories]

        space_consumption = 0
        while len(directories) > 0:
            directory = directories.pop(0)

            directories.extend(directory.get_directories())

            for file_info in directory.get_file_infos():
                space_consumption += file_info['contentLength']

        return space_consumption

    def _update_directory_structure(self):
        #Update the directory structure with online files and local directories
        online_files = [l[0].as_dict() for l in self.bucket_api.ls()]#self.bucket_api.list_file_names()['files']
        self._directories.update_structure(online_files, self.local_directories)

    def _remove_local_file(self, path, delete_online=True):
        if path in self.open_files.keys():
            self.open_files[path].delete(delete_online)
            del self.open_files[path]
        elif delete_online:
            file_info = self._directories.get_file_info(path)
            self.bucket_api.delete_file_version(file_info['fileId'], file_info['fileName'])
#{'size': 19, 'action': u'upload', 'uploadTimestamp': 1477072704000, 'fileName': u'.goutputstream-J5ZNPY', 'fileId': u'4_z4a4089f903fbc1d150640114_f104e0f44e7832f51_d20161021_m175824_c001_v0001033_t0031'}

#{u'contentType': u'application/octet-stream', u'contentSha1': u'a67ce81bd43149c12151e0a6cf1f40bc8571dfd7', u'contentLength': 19, u'fileName': u'.goutputstream-J5ZNPY', u'action': u'upload', u'fileInfo': {}, u'size': 19, u'uploadTimestamp': 1477072704000, u'fileId': u'4_z4a4089f903fbc1d150640114_f104e0f44e7832f51_d20161021_m175824_c001_v0001033_t0031'}

    def _remove_start_slash(self, path):
        if path.startswith("/"):
            path = path[1:]
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        self.logger.debug("Access %s (mode:%s)", path, mode)
        path = self._remove_start_slash(path)

        #Return access granted if path is a directory
        if self._directories.is_directory(path):
            return

        #Return access granted if path is a file
        if self._exists(path):
            return

        raise FuseOSError(errno.EACCES)

    #def chmod(self, path, mode):
    #    self.logger.debug("Chmod %s (mode:%s)", path, mode)

    #def chown(self, path, uid, gid):
    #    self.logger.debug("Chown %s (uid:%s gid:%s)", path, uid, gid)

    def getattr(self, path, fh=None):
        self.logger.debug("Get attr %s", path)
        self.logger.debug("Memory used %s", round(self._get_memory_consumption(), 2))
        path = self._remove_start_slash(path)

        #Check if path is a directory
        if self._directories.is_directory(path):
            return dict(
                st_mode=(S_IFDIR | 0777),
                st_ctime=time(),
                st_mtime=time(),
                st_atime=time(),
                st_nlink=2
            )
            
        #Check if path is a file
        elif self._exists(path):
            #If file exist return attributes

            online_files = [l[0].file_name for l in self.bucket_api.ls()]
            
            if path in online_files:
                #print "File is in bucket"
                file_info = self._directories.get_file_info(path)
                
                seconds_since_jan1_1970 = int(file_info['uploadTimestamp']/1000.)
                return dict(
                    st_mode=(S_IFREG | 0777),
                    st_ctime=seconds_since_jan1_1970,
                    st_mtime=seconds_since_jan1_1970,
                    st_atime=seconds_since_jan1_1970,
                    st_nlink=1,
                    st_size=file_info['size']
                )

            elif path.endswith(".sha1"):
                #print "File is just a hash"
                return dict(
                    st_mode=(S_IFREG | 0444),
                    st_ctime=0,
                    st_mtime=0,
                    st_atime=0,
                    st_nlink=1,
                    st_size=42
                )

            else:
                #print "File exists only locally"
                return dict(
                    st_mode=(S_IFREG | 0777),
                    st_ctime=0,
                    st_mtime=0,
                    st_atime=0,
                    st_nlink=1,
                    st_size=len(self.open_files[path])
                )

        raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        self.logger.debug("Readdir %s", path)
        path = self._remove_start_slash(path)

        self._update_directory_structure()

        dirents = []

        def in_folder(filename):
            if filename.startswith(path):
                relative_filename = filename[len(path):]

                if relative_filename.startswith("/"):
                    relative_filename = relative_filename[1:]

                if "/" not in relative_filename:
                    return True

            return False

        #Add files found in bucket
        directory = self._directories.get_directory(path)

        online_files = map(lambda file_info: file_info['fileName'], directory.get_file_infos())
        dirents.extend(online_files)

        #Add files kept in local memory
        for filename in self.open_files.keys():
            #File already listed
            if filename in dirents:
                continue

            #File is not in current folder
            if not in_folder(filename):
                continue

            #File is a virtual hashfile
            if filename.endswith(".sha1"):
                continue

            dirents.append(filename)

        #If filenames has a prefix (relative to path) remove this
        if len(path) > 0:
            dirents = map(lambda f: f[len(path) + 1:], dirents)

        #Add hash files
        if self.enable_hashfiles:
            hashes = map(lambda fn: fn + ".sha1", dirents)
            dirents.extend(hashes)

        #Add directories
        dirents.extend(['.', '..'])
        dirents.extend(map(str, self._directories.get_directories(path)))

        return dirents

    def rmdir(self, path):
        self.logger.debug("Rmdir %s", path)
        path = self._remove_start_slash(path)

        def in_folder(filename):
            if filename.startswith(path) and filename[len(path):len(path)+1] == "/":
                relative_filename = self._remove_start_slash(filename[len(path):])

                if "/" not in relative_filename:
                    return True

            return False

        #Add files found in bucket
        online_files = [l[0].file_name for l in self.bucket_api.ls()]
        dirents = filter(in_folder, online_files)

        #Add files kept in local memory
        for filename in self.open_files.keys():
            #File already listed
            if filename in dirents:
                continue

            #File is not in current folder
            if not in_folder(filename):
                continue

            #File is a virtual hashfile
            if filename.endswith(".sha1"):
                continue

            dirents.append(filename)

        for filename in dirents:
            online_files = [(l[0].file_name, l[0].id_) for l in self.bucket_api.ls()]
            
            fileName_to_fileId = dict(online_files)
            self.api.delete_file_version(fileName_to_fileId[path], path)

            self._remove_local_file(filename)

        if self._directories.is_directory(path):
            if path in self.local_directories:
                i = self.local_directories.index(path)
                self.local_directories.pop(i)

    def mkdir(self, path, mode):
        self.logger.debug("Mkdir %s (mode:%s)", path, mode)
        path = self._remove_start_slash(path)

        self.local_directories.append(path)

        self._update_directory_structure()

    def statfs(self, path):
        self.logger.debug("Fetching file system stats %s", path)
        #Returns 1 petabyte free space, arbitrary number
        block_size = 4096 * 16
        total_block_count = 1024**4  #1 Petabyte
        free_block_count = total_block_count - self._get_cloud_space_consumption() / block_size
        return dict(
            f_bsize=block_size,
            f_blocks=total_block_count,
            f_bfree=free_block_count,
            f_bavail=free_block_count
        )

    def unlink(self, path):
        self.logger.debug("Unlink %s", path)
        path = self._remove_start_slash(path)

        if not self._exists(path, include_hash=False):
            return

        self._remove_local_file(path)

        self._update_directory_structure()

    def rename(self, old, new):
        self.logger.debug("Rename old: %s, new %s", old, new)

        old = self._remove_start_slash(old)
        new = self._remove_start_slash(new)

        if not self._exists(old):
            raise FuseOSError(errno.ENOENT)

        if self._exists(new):
            self.unlink(new)

        self.open(old, 0)
        file_size = len(self.open_files[old])
        data = self.open_files[old].read(0, file_size)
        self.release(old, 0)

        self.create(new, 0)
        self.write(new, data, 0, 0)
        self.release(new, 0)

        self.unlink(old)

    def utimens(self, path, times=None):
        self.logger.debug("Utimens %s", path)

    # File methods
    # ============

    def open(self, path, flags):
        self.logger.debug("Open %s (flags:%s)", path, flags)
        path = self._remove_start_slash(path)

        if not self._exists(path):
            raise FuseOSError(errno.EACCES)

        if path.endswith(".sha1"):
            file_info = self._directories.get_file_info(path[:-5])
            self.open_files[path] = B2HashFile(self, file_info)

        elif self.open_files.get(path) is None:
            file_info = self._directories.get_file_info(path)
            self.open_files[path] = self.B2File(self, file_info)

        self.fd += 1
        return self.fd

    def create(self, path, mode, fi=None):
        self.logger.debug("Create %s (mode:%s)", path, mode)

        path = self._remove_start_slash(path)

        file_info = {}
        file_info['fileName'] = path

        self.open_files[path] = self.B2File(self, file_info, True)  #array.array('c')

        self.fd += 1
        return self.fd

    def read(self, path, length, offset, fh):
        self.logger.debug("Read %s (len:%s offset:%s fh:%s)", path, length, offset, fh)

        return self.open_files[self._remove_start_slash(path)].read(offset, length)

    def write(self, path, data, offset, fh):
        path = self._remove_start_slash(path)

        self.open_files[path].set_dirty(True)
        self.open_files[path].write(offset, data)

        return len(data)

    def truncate(self, path, length, fh=None):
        self.logger.debug("Truncate %s (%s)", path, length)

        path = self._remove_start_slash(path)
        self.open_files[path].set_dirty(True)
        self.open_files[path].truncate(length)

    def flush(self, path, fh):
        self.logger.debug("Flush %s %s", path, fh)

        self.open_files[self._remove_start_slash(path)].upload()

    def release(self, path, fh):
        self.logger.debug("Release %s %s", path, fh)

        self.logger.debug("Flushing file in case it was dirty")
        self.flush(self._remove_start_slash(path), fh)

        self._remove_local_file(path, False)
