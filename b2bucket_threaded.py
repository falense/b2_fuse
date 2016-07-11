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
import unittest

from time import time, sleep
from Queue import LifoQueue, Empty
from threading import Lock
from collections import defaultdict

from b2bucket_cached import B2BucketCached, UploadFailed
  
class B2BucketThreaded(B2BucketCached): 
    def __init__(self, *args):
        super(B2BucketThreaded, self).__init__( *args)
        
        num_threads=50
        self.queue = LifoQueue(num_threads*2)
        
        self.file_locks = defaultdict(Lock)
        
        self.running = True
        
        self.threads = []
        print "Thread ",
        for i in xrange(num_threads):
            t = threading.Thread(target=self._file_updater)
            t.start()
            self.threads.append(t)
            
            print ".",
            
        print 
        
        self.pre_queue_lock = Lock()
        self.pre_queue_running = True
        self.pre_queue = LifoQueue(num_threads*2)
        
        self.pre_file_dict = {}
        
        self.pre_thread = threading.Thread(target=self._prepare_update)
        self.pre_thread.start()
        
        
    
    def _prepare_update(self):
        while self.pre_queue_running:
            try:
                filename, operation, data  = self.pre_queue.get(True,1)
                self.pre_file_dict[filename] = (time(), operation, data)
                self.pre_queue.task_done()
            except Empty:
                for filename, (timestamp, operation, data) in self.pre_file_dict.items():
                    if time()-timestamp > 15:
                        self.queue.put((filename, operation, data))
                    
                        del self.pre_file_dict[filename]
        
        for filename, (timestamp, operation, data) in self.pre_file_dict.items():
            self.queue.put((filename, operation, data))
            del self.pre_file_dict[filename]
            
    def _file_updater(self):
        upload_url = self._get_upload_url()
        while self.running:
            try:
                filename, operation, data  = self.queue.get(True,1)
            except Empty:
                continue
            
            
            with self.file_locks[filename]:
                print "locking", filename
                if operation == "deletion":
                    super(B2BucketThreaded,self)._delete_file(filename)
                    self.queue.task_done()
                    
                elif operation == "upload":
                    try:
                        super(B2BucketThreaded,self)._put_file(filename, data, upload_url, False)
                    except UploadFailed:
                        self.logger.error("Failed to upload %s" % filename)
                    self.queue.task_done()
                    
                else:
                    self.logger.error("Invalid operation %s on %s" % (operation, filename))
                
                print "unlocking", filename
    
    def __enter__(self):
        return self
        
    def __exit__(self, *args, **kwargs):
        self.logger.info("Waiting for all B2 requests to complete")
        
        self.logger.info("Pre-Queue contains %s elements", self.pre_queue.qsize())
        self.pre_queue.join()
        
        self.logger.info("Joining pre queue thread")
        self.pre_queue_running = False
        self.pre_thread.join()
        
        self.logger.info("Queue contains %s elements", self.queue.qsize())
        self.queue.join()
        
        self.logger.info("Joining threads")
        self.running = False
        for t in self.threads:
            t.join()
            
            
            
    def put_file(self, filename, data):
        with self.pre_queue_lock:
            print filename
            self.logger.info("Postponing upload of %s (%s)", filename, len(data))
            
            self.pre_queue.put((filename, "upload", data), True)
            
            new_file = {}
            new_file['fileName'] = filename
            new_file['fileId'] = None
            new_file['uploadTimestamp'] = time()
            new_file['action'] = 'upload'
            new_file['contentLength'] = len(data)
                
            return new_file
        
    def delete_file(self, filename):  
        with self.pre_queue_lock:
            self.logger.info("Postponing deletion of %s", filename)
            self.pre_queue.put((filename, "deletion", None),True)
            
    
    def get_file(self, *args, **kwargs):
        with self.file_locks[args[0]]:
            return super(B2BucketThreaded,self).get_file(*args, **kwargs)
    
    def idle(self):
        
        self.pre_queue.join()
        print "pre_queue is empty"
        
        self.queue.join()
        print "queue is empty"
        
        for lock in self.file_locks.values():
            lock.acquire()
        print "acquired all locks"
            
        for lock in self.file_locks.values():
            lock.release()
            

class TestBucketInterface(unittest.TestCase):
    def setUp(self):
        from utility import load_config
        config = load_config()
        
        account_id = config['accountId']
        application_key = config['applicationKey']
        bucket_id = config['bucketId']
        
        self.bucket = B2BucketThreaded(account_id, application_key, bucket_id)
    
    def tearDown(self):
        self.bucket.__exit__()
        
    def test_multi_upload(self):
        import random
        
        start = time()
        
        file_list = []
        
        print "Uploading files"
        
        for i in xrange(10):
            filename = "junk-%s.txt" % random.randint(0,100000000)
            self.bucket.put_file(filename,"ascii")
            
            file_list.append(filename)
            
        print "Finished pushing uploads"
        
        self.bucket.idle()
        
        def in_file_list(filename):
            return unicode(filename) in file_list
        
        file_list_from_bucket = self.bucket.list_dir()
        
        
        
        successfully_uploaded = filter(in_file_list, file_list_from_bucket)
        
        print file_list_from_bucket, successfully_uploaded
        
        self.assertEqual(len(successfully_uploaded), len(file_list), "Not all files were uploaded")
                
    #def test_multi_download(self):
        #file_list = self.bucket._list_dir()
        
        #print "List of files:"
        #for file_info in file_list:
            #for key, value in file_info.items():
                #print "\t %s: %s" % (key,value)
            #print 
            
            
        end = time()
        
        print "Uploads took %s seconds" % (end-start)
        
if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
    
    unittest.main(failfast=True)
