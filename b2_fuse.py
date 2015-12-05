#!/usr/bin/env python

from __future__ import with_statement
from collections import defaultdict

import os
import sys
import errno

from fuse import FUSE, FuseOSError, Operations
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from b2_python_pusher import *

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
                print "Cache hit", params
                return result
            else:
                del self.data[params]
        
        return
        

class B2Bucket(object):
    def __init__(self, account_id, application_key, bucket_id, cache_timeout=100):
        self.cache_timeout = cache_timeout
        self.cache = {}
        
        self.api_url = 'https://api.backblaze.com'
        
        
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id
        
        self.account_token, self.api_url, self.download_url = self.authorize(account_id, application_key, bucket_id)
        
        self.upload_auth_token, self.upload_url = self.get_upload_url()
        
        self.bucket_name = self.get_bucket_name(self.bucket_id)
        
       # print self.get_bucket_name(self.bucket_id)
        
        self.cached = None
        
        
        #print self.list_dir()
        
        #for f in self.list_dir():
        #    print self.get_file(f)
        
    def list_buckets(self):
        subcache_name = "list_buckets"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        if self.cache[subcache_name].get():
            return self.cache[subcache_name].get()
            
        
        # Get the upload URL
        bucket_list = call_api(
            self.api_url,
            '/b2api/v1/b2_list_buckets',
            self.account_token,
            
            { 'accountId' : self.account_id }
            )
        
        return bucket_list['buckets']
        
    def get_bucket_name(self, bucket_id):
        for bucket in self.list_buckets():
            if bucket['bucketId'] == bucket_id:
                return bucket['bucketName']
            
        return
        
        
    def get_upload_url(self):
        # Get the upload URL
        upload_info = call_api(
            self.api_url,
            '/b2api/v1/b2_get_upload_url',
            self.account_token,
            { 'bucketId' : self.bucket_id }
            )
        return upload_info['authorizationToken'], upload_info['uploadUrl']
        
    def authorize(self, account_id, application_key, bucket_id):
        
        # Authorize the account
        account_auth = call_api(
            self.api_url,
            '/b2api/v1/b2_authorize_account',
            make_account_key_auth(self.account_id, self.application_key),
            {}
            )
            
        return account_auth['authorizationToken'],account_auth['apiUrl'],account_auth['downloadUrl']
    
    def list_dir(self):
        subcache_name = "list_dir"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        if self.cache[subcache_name].get():
            return self.cache[subcache_name].get()
        
        files = call_api(self.api_url,'/b2api/v1/b2_list_file_names', self.account_token, { 'bucketId' : self.bucket_id, 'maxFileCount': 1000})
        
        result = map(lambda x: x['fileName'], files['files'])
        self.cache[subcache_name].update(result)
        return result
        
    def get_file_info(self, filename):
        subcache_name = "get_file_info"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        params = (filename)
        if self.cache[subcache_name].get(params):
            return self.cache[subcache_name].get(params)
        
        
        resp = call_api(self.api_url,'/b2api/v1/b2_list_file_names', self.account_token, { 'bucketId' : self.bucket_id, 'maxFileCount': 1,'startFileName': filename})
        

        try:
            result = resp['files'][0]
            self.cache[subcache_name].update(result, params)
            return result
        except IndexError:
            return None
        except TypeError:
            return None
            
            
    def put_file(self, filename, data):
        headers = {
            'Authorization' : self.upload_auth_token,
            'X-Bz-File-Name' : filename,
            'Content-Type' : 'text/plain',   # XXX
            'X-Bz-Content-Sha1' : hashlib.sha1(data).hexdigest()
            }
        
        if 'Content-Length' not in headers:
            headers['Content-Length'] = str(len(data))
        encoded_headers = dict(
            (k, b2_url_encode(v))
            for (k, v) in headers.iteritems()
            )
            
        with OpenUrl(self.upload_url, data, encoded_headers) as response_file:
            json_text = response_file.read()
            file_info = json.loads(json_text)
            return file_info
    
    def get_file(self, filename):
        url = self.download_url + '/file/' + self.bucket_name + '/' + filename
            
        headers = {'Authorization': self.account_token}
        encoded_headers = dict(
            (k, b2_url_encode(v))
            for (k, v) in headers.iteritems()
            )
            
        with OpenUrl(url, None, encoded_headers) as resp:
            out = resp.read()
            try:
                return json.loads(out)
            except ValueError:
                return out
        
        

def load_config():
    with open("config.yaml") as f:
        import yaml
        return yaml.load(f.read())
        

class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        
        config = load_config()
        self.bucket = B2Bucket(config['accountId'], config['applicationKey'], config['bucketId'])  
          
        self.open_files = defaultdict(bytes)
        
        self.fd = 0
        

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================


    def access(self, path, mode):
        if mode == 1 or mode == 4:
            return
        else:
            raise FuseOSError(errno.EACCES)
        print "Access", path, (mode)
        return
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    #def chmod(self, path, mode):
        #print "Chmod", path, mode
        #full_path = self._full_path(path)
        #return os.chmod(full_path, mode)

    #def chown(self, path, uid, gid):
        #print "Chown", path, uid, gid
        #full_path = self._full_path(path)
        #return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        print "Fetching attributes for ", path
        
        if path == "/":
            print "Accessing root path"
            return dict(st_mode=(S_IFDIR | 0755), st_ctime=time(),                       st_mtime=time(), st_atime=time(), st_nlink=2)
        
        else:
            path = path[1:]
            
            
            if path not in self.bucket.list_dir():
                raise FuseOSError(errno.EACCES)
                
            file_info = self.bucket.get_file_info(path)
            
            return dict(st_mode=(S_IFREG | 0755), st_ctime=file_info['uploadTimestamp'], st_mtime=file_info['uploadTimestamp'], st_atime=file_info['uploadTimestamp'], st_nlink=1, st_size=file_info['size'])


    def readdir(self, path, fh):
        print "Directory listing requested for", path
        #print path, fh
        full_path = self._full_path(path)

        dirents = ['.', '..']
        
        files = self.bucket.list_dir()
        
        dirents.extend(files)
        
        #if os.path.isdir(full_path):
            #dirents.extend(os.listdir(full_path))
            
        return dirents

    #def readlink(self, path):
        #print "Readlink", path
        #pathname = os.readlink(self._full_path(path))
        #if pathname.startswith("/"):
            ## Path name is absolute, sanitize it.
            #return os.path.relpath(pathname, self.root)
        #else:
            #return pathname


    #def mknod(self, path, mode, dev):
        #print "Mknod", path, mode, dev
        #return os.mknod(self._full_path(path), mode, dev)

    #def rmdir(self, path):
        #print "Rmdir", path
        #full_path = self._full_path(path)
        #return os.rmdir(full_path)

    #def mkdir(self, path, mode):
        #print "Mkdir", path, mode
        #return os.mkdir(self._full_path(path), mode)



    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    #def unlink(self, path):
        #print "Unlink", path
        #return os.unlink(self._full_path(path))

    #def symlink(self, name, target):
        #print "Symlink", name, target
        #return os.symlink(name, self._full_path(target))

    #def rename(self, old, new):
        #print "Rename", old, new
        #return os.rename(self._full_path(old), self._full_path(new))

    #def link(self, target, name):
        #print "Link", target, name
        #return os.link(self._full_path(target), self._full_path(name))

    #def utimens(self, path, times=None):
        #print "utimens", path
        #return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        print "Open", path, flags
        
        if path.startswith("/"):
            path = path[1:]
            
        if path not in self.bucket.list_dir():
            raise FuseOSError(errno.EACCES)
            
        if self.open_files.get(path) is None:
            try:
                self.open_files[path] = bytes(self.bucket.get_file(path))
            except:
                raise FuseOSError(errno.EACCES)
        print "File opened", path
        self.fd += 1
        return self.fd

    #def create(self, path, mode, fi=None):
        #print "Create", path, mode
        #full_path = self._full_path(path)
        #return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        if path.startswith("/"):
            path = path[1:]
        
        print "Read", path, length, offset, fh
        return self.open_files[path][offset:offset + length]

    def write(self, path, data, offset, fh):
        if path.startswith("/"):
            path = path[1:]
        print "Write", path, buf, offset
        self.open_files[path] = self.open_files[path][:offset] + data
        return len(data)


    #def truncate(self, path, length, fh=None):
        #print "Truncate", path, length
        #full_path = self._full_path(path)
        #with open(full_path, 'r+') as f:
            #f.truncate(length)

    #def flush(self, path, fh):
        #print "Flush", path, fh
        #return os.fsync(fh)

    def release(self, path, fh):
        return
        if path.startswith("/"):
            path = path[1:]
            
        print "Release", path, fh
        self.bucket.put_file(path, self.open_files[path])
        
        del self.open_files[path]
        
        ##return os.close(fh)



def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
