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

import logging
import threading
import sys
import base64
import urllib
import urllib2
import json
import unittest
import hashlib
import ssl

from time import time
from Queue import LifoQueue, Empty
from threading import Lock
from collections import defaultdict


class UploadFailed(Exception):
    pass
    
class HttpError(Exception):
    def __init__(self, url, data, headers, message, code, status):
        super(HttpError, self).__init__(message)
        
        self.url = url
        self.data = data
        self.headers = headers
        
        self.code = code
        self.status = status
        
    def __repr__(self):
        output = "Request failed"
        output += "\n"
        
        output +=  'URL: %s\n' % self.url
        output +=  'Params: %s\n' % self.data
        output +=  'Headers: %s\n' % self.headers
        output += "\n"
        
        output += "Error message: %s\n" % self.message
        output +=  "Error code: %s\n" % self.code
        output +=  "Error status: %s\n" % self.status
        return output
        
    def  __str__(self):
        return self.__repr__()

def b2_url_encode(s):
    """URL-encodes a unicode string to be sent to B2 in an HTTP header.
    """
    return urllib.quote(s.encode('utf-8'))

def b2_url_decode(s):
    """Decodes a Unicode string returned from B2 in an HTTP header.

    Returns a Python unicode string.
    """
    # Use str() to make sure that the input to unquote is a str, not
    # unicode, which ensures that the result is a str, which allows
    # the decoding to work properly.
    return urllib.unquote(str(s)).decode('utf-8')



#Basic B2 Bucket access. Not cached and not thread safe
class B2Bucket(object):
    def __init__(self, account_id, application_key, bucket_id):
        self.logger = logging.getLogger("%s.%s" % (__name__,self.__class__.__name__))
        
        self.api_url = 'https://api.backblaze.com'
        
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id
        
        self.account_token, self.api_url, self.download_url = self._authorize(account_id, application_key)
        
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
        
        return self._call_api(api_call, api_call_params)['buckets']
            
    def _get_bucket_name(self, bucket_id):
        for bucket in self._list_buckets():
            if bucket['bucketId'] == bucket_id:
                return bucket['bucketName']
            
        return
        
    def _get_upload_url(self):
        api_call = '/b2api/v1/b2_get_upload_url'
        call_params = { 'bucketId' : self.bucket_id }
        
        upload_info = self._call_api(api_call, call_params)
        
        return upload_info['authorizationToken'], upload_info['uploadUrl']
        
    def _authorize(self, account_id, application_key):
        api_call = '/b2api/v1/b2_authorize_account'
        
        url = self.api_url + api_call
        base_64_string = base64.b64encode('%s:%s' % (account_id, application_key))
        request = {"Authorization": "Basic"  + base_64_string}
        
        try:
            f = self._open_url(url, None, request)
        except HttpError as e:
            print e
            sys.exit(1)
        
        account_auth = json.loads(f.read())
    
        return account_auth['authorizationToken'], account_auth['apiUrl'], account_auth['downloadUrl']
        
    def _list_dir(self, startFilename=""):
        func_name = "_list_dir"
        func_params = (startFilename)
        api_call = '/b2api/v1/b2_list_file_names'
        call_params = { 'bucketId' : self.bucket_id, 'maxFileCount': 1000, 'startFileName': startFilename}
        
        self.logger.info("%s %s", func_name, startFilename)
        
        resp = self._call_api(api_call, call_params)
        result = resp['files']
        nextFilename = resp['nextFileName']
    
        
        while len(resp['files']) == 1000 and nextFilename.startswith(startFilename):
            call_params['startFileName'] = nextFilename
            resp = self._call_api(api_call, call_params)
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
        return self._call_api(api_call, api_call_params)


    def _get_file_versions(self, filename):
        func_name = "get_file_versions"
        self.logger.info("%s %s", func_name, filename)
        
        api_call = '/b2api/v1/b2_list_file_versions'
        api_call_params = {'bucketId' : self.bucket_id, 'startFileName': filename}

        resp = self._call_api(api_call, api_call_params)
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
        
        response_file = self._open_url(upload_url.encode('ascii'), data, encoded_headers) 
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
            resp = self._call_api(api_call, api_call_params)
            
        self.logger.info("%s File deleted (%s) ", func_name, filename)
        
    def _get_file(self, filename, byte_range=None):
        func_name = "_get_file"
        self.logger.info("%s %s (bytes=%s)", func_name, filename, byte_range)
        
        api_url = self.download_url + '/file/' + self.bucket_name + '/' + b2_url_encode(filename)
        api_call_params = {'Authorization': self.account_token}
        
        
        encoded_headers = self._encode_headers(api_call_params)
        
        if byte_range is not None:
            encoded_headers['Content-Type'] = "application/octet-stream"
            encoded_headers['Range'] = "bytes=%s-%s" % byte_range
            
        resp = self._open_url(api_url, None, encoded_headers)
        self.logger.info("%s File downloaded", func_name)
        
        return resp.read()
            
    def _call_api(self, api_path, request):
        url = self.api_url + api_path
        request_data = json.dumps(request)
        headers = { 'Authorization' : self.account_token }
        
        handle = None
        try:
            handle = self._open_url(url, request_data, headers)
            json_text = handle.read()
            request_return = json.loads(json_text)
            return request_return
            
        #B2 Server reports application error
        except HttpError as e:
            #Error code type 400
            #if int(e.status)/100 == 4:
            #    raise e
            self.logger.error("Request to B2 API failed")
            print e
            sys.exit(1)
                
        finally:
            if handle is not None:
                handle.close()
    
    def _open_url(self, url, data, headers):
        num_attempts = 3
        for attempt in range(num_attempts):
            try:
                request = urllib2.Request(url, data, headers)
                handle = urllib2.urlopen(request, timeout=30.)
                return handle
            
            #Error message from server
            except urllib2.HTTPError as e:
                print e
                
                error_text = e.read()
                error = json.loads(error_text)
            
                error_code = int(error["status"]) 
                
                if error_code / 100 == 5: 
                    self.logger.warn("Server error. Request failed, attempt %s/%s", attempt+1, num_attempts)
                
                else:
                    raise HttpError(url, data, headers, error["message"], error["code"], error["status"])
                
            #Error message from local network
            except urllib2.URLError as e:
                print e
                #API request timed out
                if str(e.reason) == "timed out":
                    self.logger.warn("Network timeout. Request failed, attempt %s/%s", attempt+1, num_attempts)
                else:
                    raise e
                    
                    
            #Error message from local network
            except ssl.SSLError as e:
                print e
                #Check if API request timed out
                if str(e.message) == "The read operation timed out":
                    self.logger.warn("SSL error. Request failed, attempt %s/%s", attempt+1, num_attempts)
                    
                else:
                    raise e
            
            sleep(1.0)
            
            
        #Something else happened, re-raise exception
        self.logger.warn("Retry attempts exhausted")
        raise RuntimeError("API not responding")
            
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
    def put_file(self, *args, **kwargs):
        return self._put_file(*args, **kwargs)
        
    def delete_file(self, *args, **kwargs):
        return self._delete_file(*args, **kwargs)
        
    def get_file(self, *args, **kwargs):
        return self._get_file(args[0], kwargs.get("byte_range"))

    #Enabling bucket and teardown
    def __enter__(self):
        return self
        
    def __exit__(self, *args, **kwargs):
        pass


class TestBucketInterface(unittest.TestCase):
    def setUp(self):
        from utility import load_config
        config = load_config()
        
        account_id = config['accountId']
        application_key = config['applicationKey']
        bucket_id = config['bucketId']
        
        self.bucket = B2Bucket(account_id, application_key, bucket_id)
    
    def tearDown(self):
        self.bucket.__exit__()
        
    def test_list_buckets(self):
        bucket_list = self.bucket._list_buckets()
        print "List of buckets:"
        for bucket in bucket_list:
            for key, value in bucket.items():
                print "\t %s: %s" % (key,value)
            print 

    def test_list_files(self):
        file_list = self.bucket._list_dir()
        
        print "List of files:"
        for file_info in file_list:
            for key, value in file_info.items():
                print "\t %s: %s" % (key,value)
            print 
        
if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
    
    unittest.main(failfast=True)
