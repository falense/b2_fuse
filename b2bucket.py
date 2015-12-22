import logging

from time import time
from collections import defaultdict

from b2_python_pusher import *

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
                value.append(new_file)
                new_item = (timestamp, value)
                self.data[key] =  new_item
                
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
            
class B2Bucket(object):
    def __init__(self, account_id, application_key, bucket_id, cache_timeout=120):
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        self.cache_timeout = cache_timeout
        self.cache = {}
        
        self.api_url = 'https://api.backblaze.com'
        
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id
        
        self.account_token, self.api_url, self.download_url = self._authorize(account_id, application_key, bucket_id)
        
        self.upload_auth_token, self.upload_url = self._get_upload_url()
        
        self.bucket_name = self._get_bucket_name(self.bucket_id)
        
    def _reset_cache(self):
        self.cache = {}
        
    def _encode_headers(self, headers):
        encoded_headers = dict(
            (k.encode('ascii'), b2_url_encode(v).encode('ascii'))
            for (k, v) in headers.iteritems()
            )
        return encoded_headers
        
    def _update_cache(self, cache_name, result, params=""):
        self.cache[cache_name].update(result, params)
        return result
        
    def _get_cache(self, cache_name, params="", cache_type=Cache):
        if self.cache.get(cache_name) is None:
            self.cache[cache_name] = cache_type(self.cache_timeout)
            
        if self.cache[cache_name].get(params) is not None:
            return self.cache[cache_name].get(params)
            
        raise 
        
    #Bucket management calls (not cached)
        
    def _list_buckets(self):
        func_name = "list_buckets"
        api_call = '/b2api/v1/b2_list_buckets'
        api_call_params = { 'accountId' : self.account_id }
        
        self.logger.info(func_name)
        
        try:
            return self._get_cache(func_name)
        except:
            resp = call_api(self.api_url, api_call, self.account_token, api_call_params)
            return self._update_cache(func_name, resp['buckets'])
            
    def _get_bucket_name(self, bucket_id):
        for bucket in self._list_buckets():
            if bucket['bucketId'] == bucket_id:
                return bucket['bucketName']
            
        return
        
    def _get_upload_url(self):
        api_call = '/b2api/v1/b2_get_upload_url'
        call_params = { 'bucketId' : self.bucket_id }
        
        upload_info = call_api(self.api_url, api_call, self.account_token, call_params)
        
        return upload_info['authorizationToken'], upload_info['uploadUrl']
        
    def _authorize(self, account_id, application_key, bucket_id):
        api_call = '/b2api/v1/b2_authorize_account'
        account_key = make_account_key_auth(self.account_id, self.application_key)
        
        account_auth = call_api(self.api_url, api_call, account_key, {})
            
        return account_auth['authorizationToken'], account_auth['apiUrl'], account_auth['downloadUrl']
        
    #File listint calls
    
    def _list_dir(self, startFilename=""):
        func_name = "_list_dir"
        func_params = (startFilename)
        api_call = '/b2api/v1/b2_list_file_names'
        call_params = { 'bucketId' : self.bucket_id, 'maxFileCount': 1000, 'startFileName': startFilename}
        
        self.logger.info("%s %s", func_name, startFilename)
        
        try:
            return self._get_cache(func_name, func_params, FileCache)
        except:
            resp = call_api(self.api_url, api_call, self.account_token, call_params)
            result = resp['files']
            nextFilename = resp['nextFileName']
            
            while len(resp['files']) == 1000 and nextFilename.startswith(startFilename):
                resp = call_api(self.api_url, api_call, self.account_token, call_params)
                result.extend(resp['files'])
                nextFilename = resp['nextFileName']
            
            return self._update_cache(func_name, result, func_params)

    def list_dir(self, path=""):
        return map(lambda x: x['fileName'], self._list_dir(path))     
        
    def get_file_info(self, filename):
        self.logger.info("get_file_info %s", filename)
        
        filtered_files = filter(lambda f: f['fileName'] == filename, self._list_dir())
        
        if len(filtered_files) > 0:
            return filtered_files[0]
        else:
            return None
        
    def get_file_info_detailed(self, filename):
        func_name = "get_file_info_detailed"
        func_params = (filename)
        api_call = '/b2api/v1/b2_get_file_info'
        api_call_params = { 'fileId' : self.get_file_info(filename)['fileId']}
        
        self.logger.info("%s %s", func_name, filename)
        
        try:
            return self._get_cache(func_name, func_params)
        except:
            file_id = filter(lambda f: f['fileName'] == filename, self._list_dir())[0]['fileId']
            resp = call_api(self.api_url, api_call, self.account_token, api_call_params)
            return self._update_cache(func_name, resp, func_params)
    
    def get_file_versions(self, filename):
        func_name = "get_file_versions"
        func_params = (filename)
        api_call = '/b2api/v1/b2_list_file_versions'
        api_call_params = {'bucketId' : self.bucket_id, 'startFileName': filename}
        
        self.logger.info("%s %s", func_name, filename)
        
        try:
            return self._get_cache(func_name, func_params)
        except:
            resp = call_api(self.api_url, api_call, self.account_token, api_call_params)
            filtered_files = filter(lambda f: f['fileName'] == filename, resp['files'])
            result = map(lambda f: f['fileId'], filtered_files)
            return self._update_cache(func_name, result, func_params)
            
    #These calls are not cached, consider for performance
            
    def delete_file(self, filename):   
        func_name = "delete_file"
        api_call = '/b2api/v1/b2_delete_file_version'
        api_call_params = {'fileName': filename}
        
        self.logger.info("%s %s", func_name, filename)
        
        file_ids = self.get_file_versions(filename)
        self.cache['_list_dir'].remove_file(filename)
        
        for file_id in file_ids:
            api_call_params['fileId'] = file_id
            resp = call_api(self.api_url, api_call, self.account_token, api_call_params)
        
    def put_file(self, filename, data):
        func_name = "put_file"
        
        self.logger.info("%s %s (len:%s)", func_name, filename, len(data))
        
        if filename in self.list_dir():
            self.delete_file(filename)
        
        headers = {
            'Authorization' : self.upload_auth_token,
            'X-Bz-File-Name' : filename,
            'Content-Type' : 'b2/x-auto', 
            'X-Bz-Content-Sha1' : hashlib.sha1(data).hexdigest(),
            'Content-Length' : str(len(data))
            }
        
        encoded_headers = self._encode_headers(headers)
        
        with OpenUrl(self.upload_url.encode('ascii'), data, encoded_headers) as response_file:
            json_text = response_file.read()
            file_info = json.loads(json_text)
            
            self.cache['_list_dir'].add_file(file_info)
            
            self.logger.info("%s Upload complete", func_name)
            return file_info
    
    def get_file(self, filename):
        func_name = "get_file"
        api_url = self.download_url + '/file/' + self.bucket_name + '/' + b2_url_encode(filename)
        api_call_params = {'Authorization': self.account_token}
        
        self.logger.info("%s %s", func_name, filename)
            
        encoded_headers = self._encode_headers(api_call_params)
            
        with OpenUrl(api_url, None, encoded_headers) as resp:
            out = resp.read()
            try:
                return json.loads(out)
            except ValueError:
                return out
        
        
