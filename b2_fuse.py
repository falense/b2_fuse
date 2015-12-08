#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict

import os
import sys
import errno
import argparse
import logging
import array
#['E2BIG', 'EACCES', 'EADDRINUSE', 'EADDRNOTAVAIL', 'EADV', 'EAFNOSUPPORT', 'EAGAIN', 'EALREADY', 'EBADE', 'EBADF', 'EBADFD', 'EBADMSG', 'EBADR', 'EBADRQC', 'EBADSLT', 'EBFONT', 'EBUSY', 'ECHILD', 'ECHRNG', 'ECOMM', 'ECONNABORTED', 'ECONNREFUSED', 'ECONNRESET', 'EDEADLK', 'EDEADLOCK', 'EDESTADDRREQ', 'EDOM', 'EDOTDOT', 'EDQUOT', 'EEXIST', 'EFAULT', 'EFBIG', 'EHOSTDOWN', 'EHOSTUNREACH', 'EIDRM', 'EILSEQ', 'EINPROGRESS', 'EINTR', 'EINVAL', 'EIO', 'EISCONN', 'EISDIR', 'EISNAM', 'EL2HLT', 'EL2NSYNC', 'EL3HLT', 'EL3RST', 'ELIBACC', 'ELIBBAD', 'ELIBEXEC', 'ELIBMAX', 'ELIBSCN', 'ELNRNG', 'ELOOP', 'EMFILE', 'EMLINK', 'EMSGSIZE', 'EMULTIHOP', 'ENAMETOOLONG', 'ENAVAIL', 'ENETDOWN', 'ENETRESET', 'ENETUNREACH', 'ENFILE', 'ENOANO', 'ENOBUFS', 'ENOCSI', 'ENODATA', 'ENODEV', 'ENOENT', 'ENOEXEC', 'ENOLCK', 'ENOLINK', 'ENOMEM', 'ENOMSG', 'ENONET', 'ENOPKG', 'ENOPROTOOPT', 'ENOSPC', 'ENOSR', 'ENOSTR', 'ENOSYS', 'ENOTBLK', 'ENOTCONN', 'ENOTDIR', 'ENOTEMPTY', 'ENOTNAM', 'ENOTSOCK', 'ENOTSUP', 'ENOTTY', 'ENOTUNIQ', 'ENXIO', 'EOPNOTSUPP', 'EOVERFLOW', 'EPERM', 'EPFNOSUPPORT', 'EPIPE', 'EPROTO', 'EPROTONOSUPPORT', 'EPROTOTYPE', 'ERANGE', 'EREMCHG', 'EREMOTE', 'EREMOTEIO', 'ERESTART', 'EROFS', 'ESHUTDOWN', 'ESOCKTNOSUPPORT', 'ESPIPE', 'ESRCH', 'ESRMNT', 'ESTALE', 'ESTRPIPE', 'ETIME', 'ETIMEDOUT', 'ETOOMANYREFS', 'ETXTBSY', 'EUCLEAN', 'EUNATCH', 'EUSERS', 'EWOULDBLOCK', 'EXDEV', 'EXFULL', '__doc__', '__name__', '__package__', 'errorcode']


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
                return result
            else:
                del self.data[params]
        
        return

class B2Bucket(object):
    def __init__(self, account_id, application_key, bucket_id, cache_timeout=5):
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        self.cache_timeout = cache_timeout
        self.cache = {}
        
        self.api_url = 'https://api.backblaze.com'
        
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id
        
        self.account_token, self.api_url, self.download_url = self.authorize(account_id, application_key, bucket_id)
        
        self.upload_auth_token, self.upload_url = self.get_upload_url()
        
        self.bucket_name = self.get_bucket_name(self.bucket_id)
        
    def _reset_cache(self):
        self.cache = {}
        
    #Bucket management calls (not cached)
        
    def list_buckets(self):
        subcache_name = "list_buckets"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        if self.cache[subcache_name].get():
            return self.cache[subcache_name].get()
            
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
        upload_info = call_api(
            self.api_url,
            '/b2api/v1/b2_get_upload_url',
            self.account_token,
            { 'bucketId' : self.bucket_id }
            )
        return upload_info['authorizationToken'], upload_info['uploadUrl']
        
    def authorize(self, account_id, application_key, bucket_id):
        account_auth = call_api(
            self.api_url,
            '/b2api/v1/b2_authorize_account',
            make_account_key_auth(self.account_id, self.application_key),
            {}
            )
            
        return account_auth['authorizationToken'],account_auth['apiUrl'],account_auth['downloadUrl']
        
    #File listint calls
    
    def _list_dir(self):
        subcache_name = "_list_dir"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        if self.cache[subcache_name].get() is not None:
            return self.cache[subcache_name].get()
        
        self.logger.info("Getting bucket filelist")
        files = call_api(self.api_url,'/b2api/v1/b2_list_file_names', self.account_token, { 'bucketId' : self.bucket_id, 'maxFileCount': 1000})
        
        result = files['files']
        self.cache[subcache_name].update(result)
        return result
    
    def list_dir(self):
        result =  map(lambda x: x['fileName'], self._list_dir())
        return result
        
    def get_file_info(self, filename):
        
        files = self._list_dir()
        filtered_files = filter(lambda f: f['fileName'] == filename, files)
        
        try:
            return filtered_files[0]
        except:
            return None
        
    def get_file_info_detailed(self, filename):
        subcache_name = "get_file_info_detailed"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        params = (filename)
        if self.cache[subcache_name].get(params) is not None:
            return self.cache[subcache_name].get(params)
        
        file_id = filter(lambda f: f['fileName'] == filename, self._list_dir())[0]['fileId']
        
        
        
        resp = call_api(self.api_url,'/b2api/v1/b2_get_file_info', self.account_token, { 'fileId' : file_id})
        #print "Versions", resp['files']

        try:
            result = resp
            self.cache[subcache_name].update(result, params)
            return result
        except:
            return None
    
    def get_file_versions(self, filename):
        subcache_name = "get_file_versions"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        params = (filename)
        if self.cache[subcache_name].get(params) is not None:
            return self.cache[subcache_name].get(params)
        
        
        resp = call_api(self.api_url,'/b2api/v1/b2_list_file_versions', self.account_token, { 'bucketId' : self.bucket_id,'startFileName': filename})
        #print "Versions", resp['files']

        try:
            filtered_files = filter(lambda f: f['fileName'] == filename, resp['files'])
            result = map(lambda f: f['fileId'], filtered_files)
            self.cache[subcache_name].update(result, params)
            return result
        except:
            return None
            
    #These calls are not cached, consider for performance
            
    def delete_file(self, filename, delete_all=True):   
        self.logger.info("Deleting %s (delete_all:%s)", filename, delete_all)
        
        
        
        file_ids = self.get_file_versions(filename)
        
        self._reset_cache()
        
        found_file = False
        for file_id in file_ids:
            resp = call_api(self.api_url,'/b2api/v1/b2_delete_file_version', self.account_token, {'fileName': filename, 'fileId': file_id})
            
            found_file = True
                
        return found_file
            
    def put_file(self, filename, data):
        self.logger.info("Uploading %s (len:%s)", filename, len(data))
        self._reset_cache()
        
        
        if filename in self.list_dir():
            self.logger.info("Deleting previous versions before upload")
            self.delete_file(filename)
        
        headers = {
            'Authorization' : self.upload_auth_token,
            'X-Bz-File-Name' : filename,
            'Content-Type' : 'b2/x-auto',   # XXX
            'X-Bz-Content-Sha1' : hashlib.sha1(data).hexdigest()
            }
        
        if 'Content-Length' not in headers:
            headers['Content-Length'] = str(len(data))
        encoded_headers = dict(
            (k.encode('ascii'), b2_url_encode(v).encode('ascii'))
            for (k, v) in headers.iteritems()
            )
        
        with OpenUrl(self.upload_url.encode('ascii'), data, encoded_headers) as response_file:
            json_text = response_file.read()
            file_info = json.loads(json_text)
            
            self._reset_cache()
            return file_info
    
    def get_file(self, filename):
        self.logger.info("Downloading %s", filename)
        url = self.download_url + '/file/' + self.bucket_name + '/' + b2_url_encode(filename)
            
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
        

class B2Fuse(Operations):
    def __init__(self, account_id = None, application_key = None, bucket_id = None):
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        config = load_config()
        
        if not account_id:
            account_id = config['accountId']
        
        if not application_key:
            application_key = config['applicationKey']
            
        if not bucket_id:
            bucket_id = config['bucketId']
            
        self.bucket = B2Bucket(account_id, application_key, bucket_id)  
          
        self.open_files = defaultdict(bytes)
        self.dirty_files = set()
        
        self.fd = 0
        
        
        
    # Filesystem methods
    # ==================
    
    def _exists(self, path):
        if path in self.bucket.list_dir():
            #print "File %s exists" % path
            return True
        if path in self.open_files.keys():
            #print "File %s exists" % path
            return True
            
        #print "File %s does not exist exists" % path
        return False
        
    def access(self, path, mode):
        self.logger.debug("Access %s (mode:%s)", path, mode)
        if path.startswith("/"):
            path = path[1:]
            
        if path == "":
            return 
            
        if self._exists(path):
            return 
            
        raise FuseOSError(errno.EACCES)
        
    def getattr(self, path, fh=None):
        self.logger.debug("Get attr %s", path)
        if path.startswith("/"):
            path = path[1:]
        
        if path == "":
            #print "Accessing root path"
            return dict(st_mode=(S_IFDIR | 0777), st_ctime=time(),                       st_mtime=time(), st_atime=time(), st_nlink=2)
        
        else:
            if not self._exists(path):
                #print "File does not exist, raising error (no such file)"
                raise FuseOSError(errno.ENOENT)

            else:
                if path in self.bucket.list_dir():
                    #print "File is in bucket"
                    file_info = self.bucket.get_file_info(path)
                    
                    return dict(st_mode=(S_IFREG | 0777), st_ctime=file_info['uploadTimestamp'], st_mtime=file_info['uploadTimestamp'], st_atime=file_info['uploadTimestamp'], st_nlink=1, st_size=file_info['size'])
                else:
                    #print "File exists only locally"
                    return dict(st_mode=(S_IFREG | 0777), st_ctime=0, st_mtime=0, st_atime=0, st_nlink=1, st_size=len(self.open_files[path]))

    def readdir(self, path, fh):
        self.logger.debug("Readdir %s", path)
        if path.startswith("/"):
            path = path[1:]

        dirents = ['.', '..']
        
        files = self.bucket.list_dir()
        
        dirents.extend(files)
        
        for path in self.open_files.keys():
            if path not in dirents:
                dirents.append(path)
        
            
        return dirents

    #def readlink(self, path):
        #self.logger.debug("Readlink %s", path)

    #def mknod(self, path, mode, dev):
        #self.logger.debug("Mknod %s (mode:%s dev:%s)", path, mode, dev)

    def rmdir(self, path):
        self.logger.debug("Rmdir %s", path)
        if path.startswith("/"):
            path = path[1:]
            
        self.bucket.delete_file(path)
        
        if path in self.dirty_files:
            self.dirty_files.remove(path)
        del self.open_files[path]
        
    #def mkdir(self, path, mode):
        #self.logger.debug("Mkdir %s (mode:%s)", path, mode)

    def statfs(self, path):
        self.logger.debug("Fetching file system stats %s", path)
        #Returns 1 petabyte free space, arbitrary number
        return dict(f_bsize=4096, f_blocks=1024*1024, f_bavail=1024*1024*1024*1024)

    def unlink(self, path):
        self.logger.debug("Unlink %s", path)
        if path.startswith("/"):
            path = path[1:]
            
        filename = path.split("/")[-1]
        if not filename.startswith("."):
            self.bucket.delete_file(path)
        
        if path in self.open_files.keys():
            if path in self.dirty_files:
                self.dirty_files.remove(path)
            del self.open_files[path]

    #def symlink(self, name, target):
        #self.logger.debug("Symlink %s %s", name, target)

    def rename(self, old, new):
        self.logger.debug("Rename old: %s, new %s", old, new)
        
        if old.startswith("/"):
            old = old[1:]
            
        if new.startswith("/"):
            new = new[1:]
        
        if not self._exists(old):
            raise FuseOSError(errno.ENOENT)
            
        if new != old and self._exists(new):
            raise FuseOSError(errno.EEXIST)
            
        if old in self.dirty_files:
            self.dirty_files.remove(old)
            
            
        self.open_files[new] = self.open_files[old]
        self.dirty_files.add(new)
        self.flush(new, 0)
        self.unlink(old)
        
        return 

    #def link(self, target, name):
        #self.logger.debug("Link %s %s", target, name)

    #def utimens(self, path, times=None):
        #self.logger.debug("Utimens %s", path)

    # File methods
    # ============

    def open(self, path, flags):
        self.logger.debug("Open %s (flags:%s)", path, flags)
        
        if path.startswith("/"):
            path = path[1:]
            
        #if path not in self.bucket.list_dir():
        if not self._exists(path):
            raise FuseOSError(errno.EACCES)
            
        if self.open_files.get(path) is None:
            try:
                self.open_files[path] = array.array('c',self.bucket.get_file(path))
            except:
                raise FuseOSError(errno.EACCES)
                
        self.fd += 1
        return self.fd

    def create(self, path, mode, fi=None):
        self.logger.debug("Create %s (mode:%s)", path, mode)
        if path.startswith("/"):
            path = path[1:]
            
        self.dirty_files.add(path)
            
        self.open_files[path] = array.array('c')
        
        self.fd += 1
        return self.fd

    def read(self, path, length, offset, fh):
        self.logger.debug("Read %s (len:%s offset:%s fh:%s)", path, length, offset, fh)
        if path.startswith("/"):
            path = path[1:]
        
        return self.open_files[path][offset:offset + length].tostring()

    def write(self, path, data, offset, fh):
        self.logger.debug("Write %s (len:%s offset:%s)", path, len(data), offset)
        if path.startswith("/"):
            path = path[1:]
            
        self.dirty_files.add(path)
        
        #self.open_files[path] = self.open_files[path][:offset] + data
        self.open_files[path].extend(data)
        return len(data)


    def truncate(self, path, length, fh=None):
        self.logger.debug("Truncate %s (%s)", path, length)
        if path.startswith("/"):
            path = path[1:]
            
        self.dirty_files.add(path)
            
        self.open_files[path] = self.open_files[path][:length]

    def flush(self, path, fh):
        self.logger.debug("Flush %s %s", path, fh)
        if path.startswith("/"):
            path = path[1:]
            
        if path not in self.dirty_files:
            #print "\tFile clean"
            return 
            
        filename = path.split("/")[-1]
        if not filename.startswith("."):
            #print "\tFile dirty, has to re-upload"
            self.bucket.put_file(path, self.open_files[path])
        #else:
            #print "\tSkipping hidden file"
        
    
        self.dirty_files.remove(path)

    def release(self, path, fh):
        self.logger.debug("Release %s %s", path, fh)
        if path.startswith("/"):
            path = path[1:]
            
        self.logger.debug("Flushing file in case it was dirty")
        self.flush(path,fh)


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint for the B2 bucket")
    
    parser.add_argument("--account_id", type=str, default=None, help="Account ID for your B2 account (overrides config)")
    parser.add_argument("--application_key", type=str, default=None, help="Application key for your account  (overrides config)")
    parser.add_argument("--bucket_id", type=str, default=None, help="Bucket ID for the bucket to mount (overrides config)")
    return parser
    
def main(mountpoint, account_id, application_key, bucket_id):
    FUSE(B2Fuse(account_id, application_key, bucket_id), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    parser = create_parser()
    args = parser.parse_args()
    main(args.mountpoint, args.account_id, args.application_key, args.bucket_id)
