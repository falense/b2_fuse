from b2fuse import *

import unittest

import os
import shutil

def init_b2fuse():
    config = load_config("config.yaml")
    
    os.makedirs("mountpoint")
    
    filesystem = B2Fuse(
        config["accountId"], config["applicationKey"], config["bucketId"],
        config["enableHashfiles"], config["memoryLimit"], config["tempFolder"], config["useDisk"]
    )
    
    fuse = FUSE(filesystem, "mountpoint", nothreads=True, foreground=False)
        
    return fuse
        

class TestCreateFile(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._file_path = os.path.join(self._mountpoint, "dummy_file_create")
        
    def tearDown(self):
        os.remove(self._file_path)

    def test_create_file(self):
        f = open(self._file_path, "w")
        f.close()
            
        self.assertTrue(os.path.exists(self._file_path), "File was not created")
    
    
class TestDeleteFile(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        self._file_path = os.path.join(self._mountpoint, "dummy_file_delete")
        
        f = open(self._file_path, "w")
        f.close()
        
        
    def test_delete_file(self):       
        os.remove(self._file_path)
        
        self.assertTrue(not os.path.exists(self._file_path), "File was not deleted")
        
class TestCreateAndWrite(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._file_path = os.path.join(self._mountpoint, "dummy_file_createandwrite")
        
    def tearDown(self):
        os.remove(self._file_path)

    def test_create_file(self):
        data = "Hello world"
        f = open(self._file_path, "w")
        f.write(data)
        f.close()
        
        if os.path.exists(self._file_path):
            f = open(self._file_path, "r")
            read_data = f.read()
            f.close()
            
        self.assertEqual(data, read_data, "Written data was not the same as read data")
        
class TestCreateAndRandomWrite(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._file_path = os.path.join(self._mountpoint, "dummy_file_createandrandomwrite")
        
    def tearDown(self):
        os.remove(self._file_path)

    def test_create_file(self):
        data = "Hello world"
        
        offset = 2
        mask = "ee"
        
        read_data_orig = data[:offset] + mask + data[offset+len(mask):]
        
        f = open(self._file_path, "w")
        f.write(data)
        f.close()
        
        if os.path.exists(self._file_path):
            f = open(self._file_path, "r+")
            f.seek(offset, 0)
            f.write(mask)
            f.close()
            
        
        if os.path.exists(self._file_path):
            f = open(self._file_path, "r")
            read_data = f.read()
            f.close()
            
        print data, read_data, read_data_orig
            
        self.assertEqual(read_data_orig, read_data, "Written data was not the same as read data")
        
class TestCreateWriteCopy(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._file_path = os.path.join(self._mountpoint, "dummy_file_createwritecopy")
        self._alternate_file_path = os.path.join(self._mountpoint, "dummy_file_createwritecopy2")
        
        self._data = "Hello world"
        f = open(self._file_path, "w")
        f.write(self._data)
        f.close()

    def tearDown(self):
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
        if os.path.exists(self._alternate_file_path):
            os.remove(self._alternate_file_path)

    def test_create_file(self):
        shutil.copyfile(self._file_path, self._alternate_file_path)
        
        if os.path.exists(self._alternate_file_path):
            f = open(self._alternate_file_path, "r")
            read_data = f.read()
            f.close()
            
        self.assertEqual(self._data, read_data, "Copied filed did not contain same data as original")
        
class TestCreateWriteMove(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._file_path = os.path.join(self._mountpoint, "dummy_file_createwritemove")
        self._alternate_file_path = os.path.join(self._mountpoint, "dummy_file_createwritemove2")
        
        self._data = "Hello world"
        f = open(self._file_path, "w")
        f.write(self._data)
        f.close()

    def tearDown(self):
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
        if os.path.exists(self._alternate_file_path):
            os.remove(self._alternate_file_path)

    def test_create_file(self):
        os.rename(self._file_path, self._alternate_file_path)
        
        if os.path.exists(self._alternate_file_path):
            f = open(self._alternate_file_path, "r")
            read_data = f.read()
            f.close()
            
        self.assertEqual(self._data, read_data, "Copied filed did not contain same data as original")

class TestCreateFolder(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._folder = os.path.join(self._mountpoint, "dummy_folder")

    def tearDown(self):
        if os.path.exists(self._folder):
            os.rmdir(self._folder)

    def test_create_file(self):
        os.makedirs(self._folder)
            
        self.assertTrue(os.path.exists(self._folder), "Folder was not created")


class TestCreateFileInFolder(unittest.TestCase):
    def setUp(self):
        self._mountpoint = "mountpoint"
        
        self._folder = os.path.join(self._mountpoint, "dummy_folder_createfile")
        self._file_path = os.path.join(self._folder, "dummy_file_create")
        
    def tearDown(self):
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
        
        if os.path.exists(self._folder):
            os.rmdir(self._folder)

    def test_create_file(self):
        os.makedirs(self._folder)
        
        f = open(self._file_path, "w")
        f.close()
            
        self.assertTrue(os.path.exists(self._file_path), "File was not created")
    

if __name__ == '__main__':
    unittest.main()
