from b2bucket_cached import B2BucketCached

from b2fuse import load_config

import unittest
import hashlib

class TestB2BucketCached(unittest.TestCase):
    def setUp(self):
        config = load_config()
            
        account_id = config['accountId']
        application_key = config['applicationKey']
        bucket_id = config['bucketId']
        
        self.bucket = B2BucketCached(account_id, application_key, bucket_id) 
    
    
    #def test_list_dir(self): 
    
    def test_upload_dowload(self):
        test_data = "ascii"
        test_filename = hashlib.sha1(test_data).hexdigest()
        
        self.bucket.put_file(test_filename,test_data)
        
        downloaded_data = self.bucket.get_file(test_filename)
        
        self.assertEqual(test_data, downloaded_data, "Mismatch upload download")
        
         
    #def get_file_info(self, filename):
            
    #def get_file_info_detailed(self, filename):
    
    #def get_file_versions(self, filename):

    #def delete_file(self,  filename, *args):  
    
    #def put_file(self, *args):
            
    #def get_file(self, *args):

if __name__ == '__main__':
    unittest.main()
