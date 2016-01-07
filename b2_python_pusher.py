#! /usr/bin/python
######################################################################
# 
# File: b2_python_pusher
#
# Copyright 2015 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import base64
import hashlib
import json
import os
import sys
import urllib
import urllib2

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

def hex_sha1_of_file(path):
    with open(path, 'r') as f:
        block_size = 1024 * 1024
        digest = hashlib.sha1()
        while True:
            data = f.read(block_size)
            if len(data) == 0:
                break
            digest.update(data)
        return digest.hexdigest()

class OpenUrl(object):
    """
    Context manager that handles an open urllib2.Request, and provides
    the file-like object that is the response.
    """

    def __init__(self, url, data, headers):
        self.url = url
        self.data = data
        self.headers = headers
        self.file = None

    def __enter__(self):
        try:
            request = urllib2.Request(self.url, self.data, self.headers)
            self.file = urllib2.urlopen(request)
            return self.file
        except urllib2.HTTPError as e:
            print 'Error returned from server:'
            print
            print 'URL:', self.url
            print 'Params:', self.data
            print 'Headers:', self.headers
            print
            print e.read()
            sys.exit(1)

    def __exit__(self, exception_type, exception, traceback):
        if self.file is not None:
            self.file.close()

def call_api(url_base, api_path, auth_token, request):
    """
    Calls one API by sending JSON and parsing the JSON that comes
    back. 
    """
    url = url_base + api_path
    request_data = json.dumps(request)
    headers = { 'Authorization' : auth_token }
    with OpenUrl(url, request_data, headers) as f:
        json_text = f.read()
        return json.loads(json_text)

def post_file(url, headers, file_path):
    """Posts the contents of the file to the URL.

    URL-encodes all of the data in the headers before sending.
    """
    with open(file_path, 'r') as data_file:
        if 'Content-Length' not in headers:
            headers['Content-Length'] = str(os.path.getsize(file_path))
        encoded_headers = dict(
            (k, b2_url_encode(v))
            for (k, v) in headers.iteritems()
            )
        with OpenUrl(url, data_file, encoded_headers) as response_file:
            json_text = response_file.read()
            return json.loads(json_text)

def make_account_key_auth(account_id, application_key):
    base_64_string = base64.b64encode('%s:%s' % (account_id, application_key))
    return 'Basic ' + base_64_string


