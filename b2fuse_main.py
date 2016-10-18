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

from collections import defaultdict

import os
import sys
import errno
import logging
import array
import shutil

from fuse import FUSE, FuseOSError, Operations
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from buckettypes.b2bucket import B2Bucket
from buckettypes.b2bucket_cached import B2BucketCached

from filetypes.B2SparseFileMemory import B2SparseFileMemory
from filetypes.B2SequentialFileMemory import B2SequentialFileMemory
from filetypes.B2HashFile import B2HashFile

class DirectoryStructure(object):
    def __init__(self):
        self._folders = {}
        
    def update_structure(self, file_list, local_directories):
        folder_list = map(lambda f: f.split("/")[:-1], file_list)
        folder_list.extend(map(lambda f: f.split("/"), local_directories))
        
        self._folders = {}
        for folder in folder_list:
            self._lookup(self._folders, folder,True)
            
    def _lookup(self, folders, path, update=False):
        if len(path) == 0:
            return folders
            
        head = path.pop(0)
        if update and folders.get(head) is None:
            folders[head] = {}
        
        if folders.get(head) is not None:
            return self._lookup(folders[head], path, update)
        else:
            return None
        
    def is_directory(self, path):
        return self.get_directories(path) is not None
            
    def get_directories(self, path):
        if len(path) == 0:
            return self._folders.keys()
        else:
            path_split = path.split("/")
            r = self._lookup(self._folders, path_split)
            
            if r is not None:
                return r.keys()
            else:
                return None
                
    def get_file_info(self, path):
        return

from b2.account_info.in_memory import InMemoryAccountInfo
from b2.api import B2Api
from b2.bucket import Bucket

B2File = B2SequentialFileMemory

#General cache used for B2Bucket
class Cache(object):
    def __init__(self, cache_timeout):
        self.data = {}
        
        self.cache_timeout = cache_timeout
        
    def update(self, result, params = ""):
        self.data[params] = (time(), result)
        
    def get(self, params = ""):
        if self.data.get(params) is not None:
            entry_time, result = self.data.get(params)
            if time() - entry_time < self.cache_timeout:
                return result
            else:
                del self.data[params]
        
        return

class CacheNotFound(BaseException):
    pass

class CachedBucket(Bucket):
    def __init__(self, api, bucket_id):
        super(CachedBucket, self).__init__(api, bucket_id)
        
        self._cache = {}
        
        self._cache_timeout = 120
        
    def _reset_cache(self):
        self._cache = {}
        
    def _update_cache(self, cache_name, result, params=""):
        self._cache[cache_name].update(result, params)
        return result
        
    def _get_cache(self, cache_name, params="", cache_type=Cache):
        if self._cache.get(cache_name) is None:
            self._cache[cache_name] = cache_type(self._cache_timeout)
            
        if self._cache[cache_name].get(params) is not None:
            return self._cache[cache_name].get(params)
            
        raise CacheNotFound()
    
    def list_file_names(self):
        func_name = "list_file_names"
        
        try:
            return self._get_cache(func_name)
        except CacheNotFound:
            result = super(CachedBucket, self).list_file_names() 
            return self._update_cache(func_name, result)

    
class B2Fuse(Operations):
    def __init__(self, account_id, application_key, bucket_id, enable_hashfiles, memory_limit, temp_folder, use_disk ):
        account_info = InMemoryAccountInfo()
        self.api = B2Api(account_info)
        self.api.authorize_account('production', account_id, application_key)
        self.bucket_api = CachedBucket(self.api, bucket_id)
        
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        self.bucket = B2BucketWrapper(account_id, application_key, bucket_id)  
            
        self.enable_hashfiles = enable_hashfiles
        self.memory_limit = memory_limit
        self.temp_folder = temp_folder
        self.use_disk = use_disk
        
        if self.use_disk:
            if os.path.exists(self.temp_folder):
                shutil.rmtree(self.temp_folder)
            os.makedirs(self.temp_folder)
            
        self.directories = DirectoryStructure()
        self.local_directories = []
          
        self.open_files = defaultdict(B2File)
        
        self.fd = 0
        
    def __enter__(self):
        return self
        
    def __exit__(self, *args, **kwargs):
        self.bucket.__exit__()
        
    # Filesystem methods
    # ==================
    
    def _exists(self, path, include_hash=True):
        #Handle hash files
        if include_hash and path.endswith(".sha1"):
            path = path[:-5]
        
        #File is in bucket
        
        online_files = [file['fileName'] for file in self.bucket_api.list_file_names()['files']]
        if path in online_files:
            return True
        
        #File is open (but possibly not in bucket)
        if path in self.open_files.keys():
            return True
        
        return False
        
    def _get_memory_consumption(self):
        open_file_sizes = map(lambda f: len(f), self.open_files.values())
        
        memory = sum(open_file_sizes)
        
        return float(memory)/(1024*1024)
        
    def access(self, path, mode):
        self.logger.debug("Access %s (mode:%s)", path, mode)
        path = self._remove_start_slash(path)
            
        #Return access granted if path is a directory
        if self.directories.is_directory(path):
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
        self.logger.debug("Memory used %s", round(self._get_memory_consumption(),2))
        path = self._remove_start_slash(path)
        
        #Check if path is a directory
        if self.directories.is_directory(path):
            return dict(st_mode=(S_IFDIR | 0777), st_ctime=time(), st_mtime=time(), st_atime=time(), st_nlink=2)
            
        #Check if path is a file
        elif self._exists(path):
            #If file exist return attributes
            
            online_files = [file['fileName'] for file in self.bucket_api.list_file_names()['files']]
            if path in online_files:
                #print "File is in bucket"
                file_info = self.bucket.get_file_info(path)
                return dict(st_mode=(S_IFREG | 0777), st_ctime=file_info['uploadTimestamp'], st_mtime=file_info['uploadTimestamp'], st_atime=file_info['uploadTimestamp'], st_nlink=1, st_size=file_info['size'])
                
            elif path.endswith(".sha1"):
                #print "File is just a hash"
                return dict(st_mode=(S_IFREG | 0444), st_ctime=0, st_mtime=0, st_atime=0, st_nlink=1, st_size=42)
                
            else:
                #print "File exists only locally"
                return dict(st_mode=(S_IFREG | 0777), st_ctime=0, st_mtime=0, st_atime=0, st_nlink=1, st_size=len(self.open_files[path]))

        raise FuseOSError(errno.ENOENT)
        
    def readdir(self, path, fh):
        self.logger.debug("Readdir %s", path)
        path = self._remove_start_slash(path)

        #Update the local filestructure
        online_files = [file['fileName'] for file in self.bucket_api.list_file_names()['files']]
        
        self.directories.update_structure(online_files + self.open_files.keys(), self.local_directories)
         
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
        
        online_files = [file['fileName'] for file in self.bucket_api.list_file_names()['files']]
        for filename in online_files:
            if in_folder(filename):
                dirents.append(filename)
        
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
            dirents = map(lambda f: f[len(path)+1:], dirents)
                
        #Add hash files
        if self.enable_hashfiles:
            hashes = map(lambda fn: fn + ".sha1", dirents)
            dirents.extend(hashes)
        
        #Add directories
        dirents.extend(['.', '..'])
        dirents.extend(self.directories.get_directories(path))
        
        return dirents

    def rmdir(self, path):
        self.logger.debug("Rmdir %s", path)
        path = self._remove_start_slash(path)
            
        def in_folder(filename):
            if filename.startswith(path):
                relative_filename = self._remove_start_slash(filename[len(path):])
                
                if "/" not in relative_filename:
                    return True
            
            return False
            
        #Add files found in bucket
        online_files = [file['fileName'] for file in self.bucket_api.list_file_names()['files']]
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
            online_files = [(f['fileName'], f['fileId']) for f in self.bucket_api.list_file_names()['files']]
            fileName_to_fileId = dict(online_files)
            self.api.delete_file_version(fileName_to_fileId[path], path)
            
            self._remove_local_file(filename)
                
        if self.directories.is_directory(path):
            if path in self.local_directories:
                i =  self.local_directories.index(path)
                self.local_directories.pop(i)
        
    def mkdir(self, path, mode):
        self.logger.debug("Mkdir %s (mode:%s)", path, mode)
        path = self._remove_start_slash(path)
        
        self.local_directories.append(path)
        
        #Update the local filestructure
        
        online_files = [(f['fileName'], f['fileId']) for f in self.bucket_api.list_file_names()['files']]
        self.directories.update_structure(online_files + self.open_files.keys(), self.local_directories)
        
    def statfs(self, path):
        self.logger.debug("Fetching file system stats %s", path)
        #Returns 1 petabyte free space, arbitrary number
        return dict(f_bsize=4096*16, f_blocks=1024**4, f_bfree=1024**4, f_bavail=1024**4)

    def _remove_local_file(self, path):
        if path in self.open_files.keys():
            self.open_files[path].delete()
            
            del self.open_files[path]
            

    def unlink(self, path):
        self.logger.debug("Unlink %s", path)
        path = self._remove_start_slash(path)
            
        if not self._exists(path, include_hash=False):
            return
            
            
        online_files = [(f['fileName'], f['fileId']) for f in self.bucket_api.list_file_names()['files']]
        fileName_to_fileId = dict(online_files)
        self.api.delete_file_version(fileName_to_fileId[path], path)
        
        
        self._remove_local_file(path)


    def rename(self, old, new):
        self.logger.debug("Rename old: %s, new %s", old, new)
        
        old = self._remove_start_slash(old)
        new = self._remove_start_slash(new)
        
        if not self._exists(old):
            raise FuseOSError(errno.ENOENT)
            
        if self._exists(new):
            self.unlink(new)
            
            
        self.open(old,0)
        data = self.open_files[old]
        self.release(old,0)
            
        self.create(new,0)
        self.write(new, self.open_files[old], 0, 0)
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
            self.open_files[path] = B2HashFile(self, path)
            
        elif self.open_files.get(path) is None:
            self.open_files[path] = B2File(self, path)
  
        self.fd += 1
        return self.fd

    def create(self, path, mode, fi=None):
        self.logger.debug("Create %s (mode:%s)", path, mode)
            
        path = self._remove_start_slash(path)
        self.open_files[path] = B2File(self, path, True) #array.array('c')
        
        self.fd += 1
        return self.fd

    def read(self, path, length, offset, fh):
        self.logger.debug("Read %s (len:%s offset:%s fh:%s)", path, length, offset, fh)
        
        return self.open_files[self._remove_start_slash(path)].read(offset, length).tostring()

    def write(self, path, data, offset, fh):
        path = self._remove_start_slash(path)
        
        self.open_files[path].set_dirty(True)
        self.open_files[path].write(offset, data)
        
        return len(data)

    def truncate(self, path, length, fh=None):
        self.logger.debug("Truncate %s (%s)", path, length)
            
        path = self._remove_start_slash(path)
        self.open_files[path].set_dirty(True)
        self.open_files[path].truncate(length)# = self.open_files[path][:length]

    def flush(self, path, fh):
        self.logger.debug("Flush %s %s", path, fh)
        
        self.open_files[self._remove_start_slash(path)].upload()

    def release(self, path, fh):
        self.logger.debug("Release %s %s", path, fh)
            
        self.logger.debug("Flushing file in case it was dirty")
        self.flush(self._remove_start_slash(path),fh)

    def _remove_start_slash(self, path):
        if path.startswith("/"):
            path = path[1:]
        return path
        

    
