import logging
import threading

from time import time
from Queue import LifoQueue, Empty
from threading import Lock
from collections import defaultdict

from b2bucket import B2Bucket
  
#Threaded operations, memory only
class B2BucketThreaded(B2Bucket): 
    def __init__(self, *args):
        super(B2BucketThreaded, self).__init__( *args)
        
        num_threads=50
        self.upload_queue = LifoQueue(num_threads*2)
        self.delete_queue = LifoQueue(num_threads*2)
        
        self.file_locks = defaultdict(Lock)
        
        self.running = True
        
        self.upload_threads = []
        print "Starting upload thread ",
        for i in xrange(num_threads):
            t = threading.Thread(target=self._file_pusher)
            t.start()
            self.upload_threads.append(t)
            
            print ".",
            
        print 
            
        self.delete_threads = []
        print "Starting delete thread ",
        for i in xrange(num_threads):
            t = threading.Thread(target=self._file_deleter)
            t.start()
            self.delete_threads.append(t)
            
            print ".",
            
        print 
    
    def _file_pusher(self):
        while self.running:
            try:
                filename, data  = self.upload_queue.get(True,1)
            except Empty:
                continue
            
            super(B2BucketThreaded,self)._put_file(filename, data)
            self.upload_queue.task_done()
            self.file_locks[filename].release()
                
    def _file_deleter(self):        
        while self.running:
            try:
                filename  = self.delete_queue.get(True,1)
            except Empty:
                continue
            
            super(B2BucketThreaded,self)._delete_file(filename)
            self.delete_queue.task_done()
            self.file_locks[filename].release()
    
    def __enter__(self):
        return self
        
    def __exit__(self, *args, **kwargs):
        
        self.logger.info("Waiting for all B2 requests to complete")
        self.sync()
        
        self.running = False
        
        self.logger.info("Joining upload threads")
        for t in self.upload_threads:
            t.join()
            
        self.logger.info("Joining deletion threads")
        for t in self.delete_threads:
            t.join()
            
    def put_file(self, filename, data):
        self.logger.info("Postponing upload of %s (%s)", filename, len(data))
        self.file_locks[filename].acquire()
        
        self.upload_queue.put((filename, data), True)
        
        new_file = {}
        new_file['fileName'] = filename
        new_file['fileId'] = None
        new_file['uploadTimestamp'] = time()
        new_file['action'] = 'upload'
        new_file['contentLength'] = len(data)
            
        return new_file
        
    def delete_file(self, filename):  
        self.file_locks[filename].acquire()
         
        self.logger.info("Postponing deletion of %s", filename)
        self.delete_queue.put((filename),True)
        
    
    def get_file(self, filename):
        with self.file_locks[filename]:
            return super(B2BucketThreaded,self).get_file(filename)
    
    def sync(self):
        self.logger.info("Upload queue contains %s elements", self.upload_queue.qsize())
        self.logger.info("Deletion queue contains %s elements",  self.delete_queue.qsize())
        self.upload_queue.join()
        self.delete_queue.join()
        

        
if __name__=="__main__":
    from b2_fuse import load_config
    import random 
            
    config = load_config()
        
    account_id = config['accountId']
    application_key = config['applicationKey']
    bucket_id = config['bucketId']
   
    with B2BucketThreaded(account_id, application_key, bucket_id) as bucket:
        start = time()
        for i in xrange(50):
            filename = "junk-%s.txt" % random.randint(0,100000000)
            bucket.put_file(filename,"ascii")
        
    end = time()
    
    print "Uploads took %s seconds" % (end-start)
    
    print "Files in bucket %s" % len(bucket.list_dir())
