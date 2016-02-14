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

from __future__ import with_statement

import os
import sys
import errno
import argparse
import logging
import array
import hashlib

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from b2bucket_threaded import B2BucketThreaded
from errno import EACCES
from threading import Lock
from collections import defaultdict


def load_config():
    with open("config.yaml") as f:
        import yaml
        return yaml.load(f.read())

class B2Local(Operations):
    def __init__(self, local_root, account_id = None, application_key = None, bucket_id = None):
        self.local_root = local_root
        self.rwlock = Lock()
        
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        config = load_config()
        
        if not account_id:
            account_id = config['accountId']
        
        if not application_key:
            application_key = config['applicationKey']
            
        if not bucket_id:
            bucket_id = config['bucketId']
            
        self.bucket = B2BucketThreaded(account_id, application_key, bucket_id)  
        
        
    def access(self, path, mode):
        path = os.path.join(self.local_root, path[1:])
        
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    def chmod(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.chmod(path, *args)
        
    def chown(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.chown(path, *args)

    def create(self, path, mode):
        path = os.path.join(self.local_root, path[1:])
        return os.open(path, os.O_WRONLY | os.O_CREAT, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        return os.fsync(fh)

    def getattr(self, path, fh=None):
        path = os.path.join(self.local_root, path[1:])
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    #getxattr = None

    #def link(self, target, source):
    #    return os.link(source, target)

    listxattr = None
    
    
    def mknod(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.mknod(path, *args)
        
    def mkdir(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.mkdir(path, *args)
        
    def open(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.open(path, *args)
    
    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        path = os.path.join(self.local_root, path[1:])
        return ['.', '..'] + os.listdir(path)

    #readlink = os.readlink

    def release(self, path, fh):
        r = os.close(fh)
        
        if path.startswith("/"):
            path = path[1:]
        
        abs_path = os.path.join(self.local_root, path)
        
        self.upload_file(path, abs_path)
        
        return r

    #def rename(self, old, new):
        #return os.rename(old, self.root + new)
        
    def rmdir(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.rmdir(path, *args)

    def statfs(self, path):
        path = os.path.join(self.local_root, path[1:])
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    #def symlink(self, target, source):
        #return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        path = os.path.join(self.local_root, path[1:])
        with open(path, 'r+') as f:
            f.truncate(length)

    def unlink(self, path, *args):
        self.bucket.delete_file(path[1:])
        
        path = os.path.join(self.local_root, path[1:])
        return os.unlink(path, *args)
    
    def utimens(self, path, *args):
        path = os.path.join(self.local_root, path[1:])
        return os.utime(path, *args)
        
    def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)
    
    def download_file(self, b2_filename, local_filename):
        data = self.bucket.get_file(b2_filename)
        
        folder = os.path.dirname(local_filename)
        if not os.path.exists(folder):
            os.mkdir(folder)
        
        with open(local_filename,"wb") as f:
            f.write(data)
            
    def upload_file(self, b2_filename, local_filename):
        with open(local_filename, "rb") as f:
            data = f.read()
            self.bucket.put_file(b2_filename, data)
    
    def sha1_file(self, local_filename):
        sha1 = hashlib.sha1()
        with open(local_filename, 'rb') as f:
            while True:
                data = f.read(65536)
                if not data:
                    break
                sha1.update(data)
                
        return sha1.hexdigest()
    
    def sync_folder(self):
        try:
            file_list = self.bucket._list_dir()
            
            for i, file_info in enumerate(file_list):
                rel_filename = file_info['fileName']
                upload_time =  file_info['uploadTimestamp']/1000
                
                filename = os.path.join(self.local_root, rel_filename)
                
                percent = (i+1)*100/len(file_list)
                if percent % 10 == 0:
                    print "{0:.0f}%".format(percent)
                
                if os.path.exists(filename):
                    continue
                    
                self.logger.debug("New file found in B2 (%s)" % rel_filename)
                
                self.download_file(rel_filename, filename)
                os.utime(filename, (upload_time,upload_time))
                    
            for i, file_info in enumerate(file_list):
                rel_filename = file_info['fileName']
                upload_time =  file_info['uploadTimestamp']/1000
                
                filename = os.path.join(self.local_root, rel_filename)
                local_modtime = getattr(os.lstat(filename), "st_mtime")
                    
                #Local copy is newer
                if local_modtime-upload_time > 60:                    
                    b2_hash = self.bucket.get_file_info_detailed(rel_filename)['contentSha1']
                    local_hash = self.sha1_file(filename)
            
                    #If hash is different, upload new file
                    if local_hash != b2_hash:
                        self.logger.debug("Local file updated, uploading (%s)" % rel_filename)
                        self.upload_file(rel_filename, filename)
                        
                        #This is done to not have rules trigger next startup
                        os.utime(filename, (time(),time()))
                    #Hash is the same, change local timestamp to avoid check in the future.
                    else:
                        self.logger.debug("Local file modtime changed, content same. Changing local modtime (%s)" % rel_filename)
                        os.utime(filename, (upload_time,upload_time))
                        
                #Online copy is newer
                elif upload_time-local_modtime > 60:                    
                    b2_hash = self.bucket.get_file_info_detailed(rel_filename)['contentSha1']
                    local_hash = self.sha1_file(filename)
                        
                    #If hashes differ, download new version from cloud
                    if local_hash != b2_hash:
                        self.download_file(rel_filename, filename)
                        
                    os.utime(filename, (upload_time,upload_time))
                
        except KeyboardInterrupt:
            pass
                    
    def __enter__(self):
        self.sync_folder()
        return self
        
    def __exit__(self, *args, **kwargs):
        self.bucket.__exit__()
    
def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("local_directory", type=str, help="Directory for local copy of data")
    parser.add_argument("mountpoint", type=str, help="Mountpoint for the B2 bucket")
    
    parser.add_argument("--account_id", type=str, default=None, help="Account ID for your B2 account (overrides config)")
    parser.add_argument("--application_key", type=str, default=None, help="Application key for your account  (overrides config)")
    parser.add_argument("--bucket_id", type=str, default=None, help="Bucket ID for the bucket to mount (overrides config)")
    return parser
    
def main(local_directory, mountpoint, account_id, application_key, bucket_id):
    with B2Local(local_directory,account_id, application_key, bucket_id) as filesystem:
        FUSE(filesystem, mountpoint, foreground=True, nothreads=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
    
    parser = create_parser()
    args = parser.parse_args()
    main(args.local_directory, args.mountpoint, args.account_id, args.application_key, args.bucket_id)
