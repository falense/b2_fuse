import logging
import threading

from time import time
from Queue import LifoQueue, Empty
from threading import Lock
from collections import defaultdict

from b2bucket import B2Bucket

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
        
#Special cache used to handle the addition and deletion of files more effciently.
class FileCache(Cache):
    def add_file(self, upload_resp):        
        new_file = {}
        new_file['fileName'] = upload_resp['fileName']
        new_file['fileId'] = upload_resp['fileId']
        new_file['uploadTimestamp'] = time()
        new_file['action'] = 'upload'
        new_file['size'] = upload_resp['contentLength']
        
        for key, (timestamp,value) in self.data.items():
            if new_file['fileName'].startswith(key):
                found = -1
                
                for i, cached_file in enumerate(value):
                    if cached_file['fileName'] == new_file['fileName']:
                        found = i
                
                if found == -1:
                    value.append(new_file)
                else:
                    value[found] = new_file
                    
                self.data[key] = (timestamp, value)
                    
                
    def remove_file(self, filename):
        for key, (timestamp,value) in self.data.items():
            found_i = -1
            for i, f in enumerate(value):
                if f['fileName'] == filename:
                    found_i = i
                    break
            
            if found_i != -1:
                timestamp,value = self.data[key]
                value.pop(found_i)
                self.data[key] = (timestamp, value)
            

class CacheNotFound(BaseException):
    pass


    

#Cached bucket, maintains local overview of bucket content
class B2BucketCached(B2Bucket):
    def __init__(self, *args):
        super(B2BucketCached, self).__init__(*args)
        
        cache_timeout=120
        self.cache_timeout = cache_timeout
        self.cache = {}
    
    
    def _reset_cache(self):
        self.cache = {}
        
    def _update_cache(self, cache_name, result, params=""):
        self.cache[cache_name].update(result, params)
        return result
        
    def _get_cache(self, cache_name, params="", cache_type=Cache):
        if self.cache.get(cache_name) is None:
            self.cache[cache_name] = cache_type(self.cache_timeout)
            
        if self.cache[cache_name].get(params) is not None:
            return self.cache[cache_name].get(params)
            
        raise CacheNotFound()
    
    
    #File listint calls

    
    def _list_dir(self, startFilename=""):
        func_name = "_list_dir"
        func_params = (startFilename)
        
        try:
            return self._get_cache(func_name, func_params, FileCache)
        except CacheNotFound:
            result = super(B2BucketCached, self)._list_dir(startFilename) 
            return self._update_cache(func_name, result, func_params)

    def list_dir(self, startFilename=""):
        return  map(lambda x: x['fileName'], self._list_dir(startFilename))   
        
    def get_file_info(self, filename):
        return filter(lambda f: f['fileName'] == filename, self._list_dir())[0]
            
    def get_file_info_detailed(self, filename):
        func_name = "get_file_info_detailed"
        func_params = (filename)
        
        try:
            return self._get_cache(func_name, func_params)
        except CacheNotFound:
            resp = super(B2BucketCached, self)._get_file_info_detailed(filename)
            return self._update_cache(func_name, resp, func_params)
    
    def get_file_versions(self, filename):
        func_name = "get_file_versions"
        func_params = (filename)
        
        try:
            return self._get_cache(func_name, func_params)
        except CacheNotFound:
            result = super(B2BucketCached, self)._get_file_versions(filename)
            return self._update_cache(func_name, result, func_params)

    def delete_file(self,  filename, *args):  
        if self.cache.get('_list_dir') is not None:
            self.cache['_list_dir'].remove_file(filename)
        return super(B2BucketCached, self)._delete_file(filename, *args)
    
    def put_file(self, *args):
        file_info = super(B2BucketCached, self)._put_file(*args)
        if self.cache.get('_list_dir') is not None:
            self.cache['_list_dir'].add_file(file_info)
        return file_info
            
    def get_file(self, *args, **kwargs):
        return self._get_file(*args, **kwargs)
