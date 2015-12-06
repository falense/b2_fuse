#!/usr/bin/env python

from __future__ import with_statement
from collections import defaultdict

import os
import sys
import errno
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
            
        if self.cache[subcache_name].get():
            return self.cache[subcache_name].get()
        
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
        
            
    def get_file_versions(self, filename):
        subcache_name = "get_file_versions"
        if self.cache.get(subcache_name) is None:
            self.cache[subcache_name] = Cache(self.cache_timeout)
            
        params = (filename)
        if self.cache[subcache_name].get(params):
            return self.cache[subcache_name].get(params)
        
        
        resp = call_api(self.api_url,'/b2api/v1/b2_list_file_versions', self.account_token, { 'bucketId' : self.bucket_id,'startFileName': filename})
        print "Versions", resp['files']

        try:
            filtered_files = filter(lambda f: f['fileName'] == filename, resp['files'])
            result = map(lambda f: f['fileId'], filtered_files)
            self.cache[subcache_name].update(result, params)
            return result
        except:
            return None
            
    #These calls are not cached, consider for performance
            
    def delete_file(self, filename, delete_all=True):   
        print "Deleting files:",
        file_ids = self.get_file_versions(filename)
        
        self._reset_cache()
        
        found_file = False
        for file_id in file_ids:
            resp = call_api(self.api_url,'/b2api/v1/b2_delete_file_version', self.account_token, {'fileName': filename, 'fileId': file_id})
            
            found_file = True
                
        return found_file
            
    def put_file(self, filename, data):
        print "Uploading file", data
        self._reset_cache()
        
        
        print "\tDeleting all previous version first"
        self.delete_file(filename)
        print "\tDeletion complete"
        
        headers = {
            'Authorization' : self.upload_auth_token,
            'X-Bz-File-Name' : filename,
            'Content-Type' : 'b2/x-auto',   # XXX
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
            
            self._reset_cache()
            return file_info
    
    def get_file(self, filename):
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
    def __init__(self):
        config = load_config()
        self.bucket = B2Bucket(config['accountId'], config['applicationKey'], config['bucketId'])  
          
        self.open_files = defaultdict(bytes)
        self.dirty_files = set()
        
        self.fd = 0
        
    # Filesystem methods
    # ==================
    
    def _exists(self, path):
        if path in self.bucket.list_dir():
            print "File %s exists" % path
            return True
        if path in self.open_files.keys():
            print "File %s exists" % path
            return True
            
        print "File %s does not exist exists" % path
        return False
        
    def access(self, path, mode):
        print "Access", path, (mode)
        if path.startswith("/"):
            path = path[1:]
            
        if path == "":
            return 
            
        if self._exists(path):
            return 
            
        raise FuseOSError(errno.EACCES)
        
    def getattr(self, path, fh=None):
        print "Fetching attributes for ", path
        if path.startswith("/"):
            path = path[1:]
        
        if path == "":
            print "Accessing root path"
            return dict(st_mode=(S_IFDIR | 0777), st_ctime=time(),                       st_mtime=time(), st_atime=time(), st_nlink=2)
        
        else:
            if not self._exists(path):
                raise FuseOSError(errno.ENOENT)

            else:
                if path in self.bucket.list_dir():
                    print "File is in bucket"
                    file_info = self.bucket.get_file_info(path)
                    
                    return dict(st_mode=(S_IFREG | 0777), st_ctime=file_info['uploadTimestamp'], st_mtime=file_info['uploadTimestamp'], st_atime=file_info['uploadTimestamp'], st_nlink=1, st_size=file_info['size'])
                else:
                    print "File exists only locally"
                    
                    return dict(st_mode=(S_IFREG | 0777), st_ctime=time(), st_mtime=time(), st_atime=time(), st_nlink=1, st_size=len(self.open_files[path]))

    def readdir(self, path, fh):
        print "Directory listing requested for", path
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
        #print "Readlink", path


    #def mknod(self, path, mode, dev):
        #print "Mknod", path, mode, dev

    def rmdir(self, path):
        print "Rmdir", path
        if path.startswith("/"):
            path = path[1:]
            
        self.bucket.delete_file(path)
        
        if path in self.dirty_files:
            self.dirty_files.remove(path)
        del self.open_files[path]
        
    #def mkdir(self, path, mode):
        #print "Mkdir", path, mode

    def statfs(self, path):
        print "Fetching file system stats", path
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def unlink(self, path):
        print "Unlink", path
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
        #print "Symlink", name, target

    def rename(self, old, new):
        print "Rename", old, new
        
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
        #print "Link", target, name

    #def utimens(self, path, times=None):
        #print "utimens", path

    # File methods
    # ============

    def open(self, path, flags):
        print "Open", path, flags
        
        if path.startswith("/"):
            path = path[1:]
            
        #if path not in self.bucket.list_dir():
        if not self._exists(path):
            raise FuseOSError(errno.EACCES)
            
        if self.open_files.get(path) is None:
            try:
                self.open_files[path] = bytes(self.bucket.get_file(path))
            except:
                raise FuseOSError(errno.EACCES)
                
        self.fd += 1
        return self.fd

    def create(self, path, mode, fi=None):
        print "Create", path, mode
        if path.startswith("/"):
            path = path[1:]
            
        self.dirty_files.add(path)
            
        self.open_files[path] = bytes()
        
        self.fd += 1
        return self.fd

    def read(self, path, length, offset, fh):
        print "Read", path, length, offset, fh
        if path.startswith("/"):
            path = path[1:]
        
        return self.open_files[path][offset:offset + length]

    def write(self, path, data, offset, fh):
        print "Write", path, data, offset
        if path.startswith("/"):
            path = path[1:]
            
        self.dirty_files.add(path)
        
        self.open_files[path] = self.open_files[path][:offset] + data
        return len(data)


    def truncate(self, path, length, fh=None):
        print "Truncate", path, length
        if path.startswith("/"):
            path = path[1:]
            
        self.dirty_files.add(path)
            
        self.open_files[path] = self.open_files[path][:length]

    def flush(self, path, fh):
        print "Flush", path, fh
        if path.startswith("/"):
            path = path[1:]
            
        if path not in self.dirty_files:
            print "\tFile clean"
            return 
            
        filename = path.split("/")[-1]
        if not filename.startswith("."):
            print "\tFile dirty, has to re-upload"
            self.bucket.put_file(path, self.open_files[path])
        else:
            print "\tSkipping hidden file"
        
    
        self.dirty_files.remove(path)

    def release(self, path, fh):
        print "Release", path, fh
        if path.startswith("/"):
            path = path[1:]
            
        print "\tIndirect ", self.flush(path,fh)



def main(mountpoint):
    FUSE(B2Fuse(), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
