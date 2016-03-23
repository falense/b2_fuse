import logging
import threading
import sys

from time import time
from Queue import LifoQueue, Empty
from threading import Lock
from collections import defaultdict

from b2_python_pusher import *

class UploadFailed(Exception):
    pass

#Basic B2 Bucket access. Not cached and not thread safe
class B2Bucket(object):
    def __init__(self, account_id, application_key, bucket_id):
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        self.api_url = 'https://api.backblaze.com'
        
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id
        
        self.account_token, self.api_url, self.download_url = self._authorize(account_id, application_key, bucket_id)
        
        self.upload_auth_token, self.upload_url = self._get_upload_url()
        
        self.bucket_name = self._get_bucket_name(self.bucket_id)

        
        
    def _encode_headers(self, headers):
        encoded_headers = dict(
            (k.encode('ascii'), b2_url_encode(v).encode('ascii'))
            for (k, v) in headers.iteritems()
            )
        return encoded_headers
        
    #Bucket management calls (not cached)
        
    def _list_buckets(self):
        func_name = "list_buckets"
        api_call = '/b2api/v1/b2_list_buckets'
        api_call_params = { 'accountId' : self.account_id }
        
        self.logger.info(func_name)
        
        
        return call_api(self.api_url, api_call, self.account_token, api_call_params)['buckets']
            
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
        
    def _list_dir(self, startFilename=""):
        func_name = "_list_dir"
        func_params = (startFilename)
        api_call = '/b2api/v1/b2_list_file_names'
        call_params = { 'bucketId' : self.bucket_id, 'maxFileCount': 1000, 'startFileName': startFilename}
        
        self.logger.info("%s %s", func_name, startFilename)
        
        resp = call_api(self.api_url, api_call, self.account_token, call_params)
        result = resp['files']
        nextFilename = resp['nextFileName']
    
        
        while len(resp['files']) == 1000 and nextFilename.startswith(startFilename):
            call_params['startFileName'] = nextFilename
            resp = call_api(self.api_url, api_call, self.account_token, call_params)
            result.extend(resp['files'])
            nextFilename = resp['nextFileName']
            
            
            self.logger.info("File listing contains %s elements (s:%s)", len(result), sys.getsizeof(result[0]))
            
        return result
        
    def _get_file_info_detailed(self, filename):
        func_name = "get_file_info_detailed"
        self.logger.info("%s %s", func_name, filename)
        
        api_call = '/b2api/v1/b2_get_file_info'
        api_call_params = { 'fileId' : self.get_file_info(filename)['fileId']}
        
        file_id = self._get_file_versions(filename)[0]['fileId']
        return call_api(self.api_url, api_call, self.account_token, api_call_params)


    def _get_file_versions(self, filename):
        func_name = "get_file_versions"
        self.logger.info("%s %s", func_name, filename)
        
        api_call = '/b2api/v1/b2_list_file_versions'
        api_call_params = {'bucketId' : self.bucket_id, 'startFileName': filename}

        resp = call_api(self.api_url, api_call, self.account_token, api_call_params)
        filtered_files = filter(lambda f: f['fileName'] == filename, resp['files'])
        
        return  filtered_files 
        
    def _put_file(self, filename, data, upload_tokenurl=None, delete_before_push=True):
        func_name = "_put_file"
        self.logger.info("%s %s (len:%s)", func_name, filename, len(data))
        
        if delete_before_push:
            self._delete_file(filename)
            
        if upload_tokenurl is None:
            upload_auth_token, upload_url = self.upload_auth_token, self.upload_url
        else:
            upload_auth_token, upload_url = upload_tokenurl
        
        headers = {
            'Authorization' : upload_auth_token,
            'X-Bz-File-Name' : filename,
            'Content-Type' : 'b2/x-auto', 
            'X-Bz-Content-Sha1' : hashlib.sha1(data).hexdigest(),
            'Content-Length' : str(len(data))
            }
        
        try:
            encoded_headers = self._encode_headers(headers)
        except UnicodeDecodeError:
            raise UploadFailed()
        
        with OpenUrl(upload_url.encode('ascii'), data, encoded_headers) as response_file:
            json_text = response_file.read()
            file_info = json.loads(json_text)
            
            self.logger.info("%s File uploaded (%s)", func_name, filename)
            
            return file_info
            
    def _delete_file(self, filename):   
        func_name = "_delete_file"
        
        file_ids = map(lambda f: f['fileId'], self._get_file_versions(filename))
        
        self.logger.info("%s %s (%s)", func_name, filename, len(file_ids))
        
        api_call = '/b2api/v1/b2_delete_file_version'
        api_call_params = {'fileName': filename}
        
        for file_id in file_ids:
            api_call_params['fileId'] = file_id
            resp = call_api(self.api_url, api_call, self.account_token, api_call_params)
            
        self.logger.info("%s File deleted (%s) ", func_name, filename)
        
    def _get_file(self, filename):
        func_name = "_get_file"
        self.logger.info("%s %s", func_name, filename)
        
        api_url = self.download_url + '/file/' + self.bucket_name + '/' + b2_url_encode(filename)
        api_call_params = {'Authorization': self.account_token}
            
        encoded_headers = self._encode_headers(api_call_params)
            
        with OpenUrl(api_url, None, encoded_headers) as resp:
            self.logger.info("%s File downloaded", func_name)
            return resp.read()
            
    #File listint calls
    def list_dir(self, path=""):
        return map(lambda x: x['fileName'], self._list_dir(path))     
        
    def get_file_info(self, filename):
        self.logger.info("get_file_info %s", filename)
        
        filtered_files = filter(lambda f: f['fileName'] == filename, self._list_dir())
        
        if len(filtered_files) > 0:
            return filtered_files[0]
        else:
            return None
        
    def get_file_info_detailed(self, *args):
        return self._get_file_info_detailed(*args)
        
    
    def get_file_versions(self, *args):
        return map(lambda f: f['fileId'], self._get_file_versions(*args))
        
    #File update calls
    def put_file(self, *args):
        return self._put_file(*args)
        
    def delete_file(self, *args):
        return self._delete_file(*args)
        
    def get_file(self, *args):
        return self._get_file(*args)
        


    def __enter__(self):
        return self
        
    def __exit__(self, *args, **kwargs):
        pass
            
