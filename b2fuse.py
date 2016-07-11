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
import argparse
import logging
import array
import shutil
import yaml

from fuse import FUSE, FuseOSError, Operations
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from b2bucket import B2Bucket
from b2bucket_cached import B2BucketCached
from b2bucket_threaded import B2BucketThreaded

from b2fuse_filetypes import *
from utility import load_config

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

B2File = B2SparseFileMemory

class B2Fuse(Operations):
    def __init__(self, account_id = None, application_key = None, bucket_id = None, enable_hashfiles=False, memory_limit=128):
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        config = load_config()
        
        if not account_id:
            account_id = config['accountId']
        
        if not application_key:
            application_key = config['applicationKey']
            
        if not bucket_id:
            bucket_id = config['bucketId']
            
        self.file_download_split = config['fileDownloadSplit']
        self.big_file_upload_split = config['bigFileUploadSplit']
        self.temp_folder = config['tempFolder']
        
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder)
        
        os.makedirs(self.temp_folder)
            
        self.bucket = B2BucketThreaded(account_id, application_key, bucket_id)  
        
        self.directories = DirectoryStructure()
        self.local_directories = []
          
        self.open_files = defaultdict(B2File)
        
        self.enable_hashfiles = enable_hashfiles
        self.memory_limit = memory_limit
        
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
        if path in self.bucket.list_dir():
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
            if path in self.bucket.list_dir():
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
        self.directories.update_structure(self.bucket.list_dir(path) + self.open_files.keys(), self.local_directories)
         
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
        bucket_files = self.bucket.list_dir()
        for filename in bucket_files:
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
        bucket_files = self.bucket.list_dir()
        dirents = filter(in_folder, bucket_files)
        
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
            self.bucket.delete_file(filename)
            if filename in self.open_files.key():
                del self.open_files[path]
        
            if filename in self.dirty_files:
                self.dirty_files.remove(filename)
                
        if self.directories.is_directory(path):
            if path in self.local_directories:
                i =  self.local_directories.index(path)
                self.local_directories.pop(i)
        
    def mkdir(self, path, mode):
        self.logger.debug("Mkdir %s (mode:%s)", path, mode)
        path = self._remove_start_slash(path)
        
        self.local_directories.append(path)
        
        #Update the local filestructure
        self.directories.update_structure(self.bucket.list_dir() + self.open_files.keys(), self.local_directories)
        
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
            
        self.bucket.delete_file(path)
        
        self._remove_local_file(path)

    #def symlink(self, name, target):
        #self.logger.debug("Symlink %s %s", name, target)

    def rename(self, old, new):
        self.logger.debug("Rename old: %s, new %s", old, new)
        
        old = self._remove_start_slash(old)
        new = self._remove_start_slash(new)
        
        if not self._exists(old):
            raise FuseOSError(errno.ENOENT)
            
        if self._exists(new):
            self.unlink(new)
            
        if old in self.dirty_files:
            self.dirty_files.remove(old)
            
        self.open(old,0)
        data = self.open_files[old]
        self.release(old,0)
            
        self.create(new,0)
        self.write(new, self.open_files[old], 0, 0)
        self.release(new, 0)
        
        self.unlink(old)

    #def link(self, target, name):
        #self.logger.debug("Link %s %s", target, name)

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
        

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint for the B2 bucket")
    
    parser.add_argument("--account_id", type=str, default=None, help="Account ID for your B2 account (overrides config)")
    parser.add_argument("--application_key", type=str, default=None, help="Application key for your account  (overrides config)")
    parser.add_argument("--bucket_id", type=str, default=None, help="Bucket ID for the bucket to mount (overrides config)")
    return parser
    
def main(mountpoint, account_id, application_key, bucket_id):
    with B2Fuse(account_id, application_key, bucket_id) as filesystem:
        FUSE(filesystem, mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
    
    parser = create_parser()
    args = parser.parse_args()
    main(args.mountpoint, args.account_id, args.application_key, args.bucket_id)
