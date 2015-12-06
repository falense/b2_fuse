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

USAGE = """
Usage:

    b2_python_pusher <accountId> <applicationKey> <bucketId> <folderName>

Backs up the current directory, and everything in it, to B2 Cloud
Storage.  

The accountId and accountKey come from your account page at
backblaze.com. 

The bucket to store files in must already exist.  You can use
the B2 command-line tool, b2, to create the bucket.

The folderName is the prefix used for files backed up from the current
folder.   If the folder name you give is "photos/backup" and one of
the files in the current directory is named "kitten.jpg", the
resulting file in B2 will be called "photos/backup/kitten.jpg".
"""

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

def back_up_directory(base_path, file_names, folder_name, 
                      bucket_id, account_token,
                      api_url, upload_url, upload_auth_token):
    # Get the info on what's already backed up: a map from file name
    # to mod time.
    current_state = {}
    state_file = base_path + '.b2_python_pusher'
    try:
        with open(state_file, 'r') as f:
            current_state = json.loads(f.read())
    except IOError as e:
        pass

    # Handle deleted files
    gone = [name for name in current_state.keys() if name not in file_names]
    for file_name in gone:
        local_file_path = base_path + file_name
        backup_file_path = folder_name + '/' + local_file_path
        call_api(
            api_url,
            '/b2api/v1/b2_hide_file',
            account_token,
            { 'bucketId' : bucket_id, 'fileName' : backup_file_path }
            )
        del current_state[file_name]
        print 'deleted: ', backup_file_path

    # Check each of the files to see if they have changed, or are new.
    for file_name in file_names:
        local_file_path = base_path + file_name
        backup_file_path = folder_name + '/' + local_file_path
        mtime = os.path.getmtime(local_file_path)
        if mtime != current_state.get(file_name, -1):
            headers = {
                'Authorization' : upload_auth_token,
                'X-Bz-File-Name' : backup_file_path,
                'Content-Type' : 'text/plain',   # XXX
                'X-Bz-Content-Sha1' : hex_sha1_of_file(local_file_path)
                }
            file_info = post_file(upload_url, headers, local_file_path)
            current_state[file_name] = mtime
            print 'uploaded:', backup_file_path

    # Save the state
    with open(state_file, 'w') as f:
        f.write(json.dumps(current_state, indent=4, sort_keys=True))
        f.write('\n')
    
def main():
    
    auth_urls = {'-production':'https://api.backblaze.com'}

    args = sys.argv[1:]
    option = '-production'
    api_url = auth_urls[option]    
    while 0 < len(args) and args[0][0] == '-':
        option = args[0]
        args = args[1:]
        if option in auth_urls:
            api_url = auth_urls[option]
            del sys.argv[1]
            break
        else:            
            print 'ERROR: unknown option', option
            print USAGE
            sys.exit(1)
    
    if len(sys.argv) != 5:
        print USAGE
        sys.exit(1)

    print 'Using %s %s' % (option[1:], api_url)

    account_id = sys.argv[1]
    application_key = sys.argv[2]
    bucket_id = sys.argv[3]
    folder_name = sys.argv[4]

    # Authorize the account
    account_auth = call_api(
        api_url,
        '/b2api/v1/b2_authorize_account',
        make_account_key_auth(account_id, application_key),
        {}
        )
    account_token = account_auth['authorizationToken']
    api_url = account_auth['apiUrl']

    # Get the upload URL
    upload_info = call_api(
        api_url,
        '/b2api/v1/b2_get_upload_url',
        account_token,
        { 'bucketId' : bucket_id }
        )
    upload_url = upload_info['uploadUrl']
    upload_auth_token = upload_info['authorizationToken']

    # Walk down through all directories, starting at the current one,
    # backing each one up.
    encoding = sys.getfilesystemencoding()
    for (dir_path, dir_names, file_names) in os.walk('.'):
        file_names = set(fn.decode(encoding) for fn in file_names)
        if '.b2_python_pusher' in file_names:
            file_names.remove('.b2_python_pusher')
        if dir_path == '.':
            base_path = ''
        else:
            assert dir_path.startswith('./')
            base_path = dir_path[2:] + '/'
        back_up_directory(base_path, file_names, folder_name,
                          bucket_id, account_token,
                          api_url, upload_url, upload_auth_token)
    
if __name__ == '__main__':
    main()
