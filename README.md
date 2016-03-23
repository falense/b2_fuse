# b2_fuse - FUSE for Backblaze B2
 
### Version: 1.2.1

#### Warning this software may contain bugs, be careful of using it with important data.
#### Please report bugs, use-case issues and feature requests through the Github issue tracker

### Basic setup:

Requires YAML and FUSE for python to work (this is not the same as "python-fuse" package). 

Install YAML for python as follows: 

```
sudo apt-get install python-yaml
```

Install FUSE for python as follows: 

```
sudo apt-get install python-pip
sudo pip install fusepy
```

An example config ("config.yaml"):

```
accountId: <youraccountid>
applicationKey: <yourapplicationid>
bucketId: <yourbucketid>
```

### Online only copy

This FUSE driver can be used either with a (small) memory backing for current open files or a complete local copy. In order to use the FUSE driver as an interface to the online service B2 (with no local copy) use:

```
python b2fuse.py <mountpoint>
```

Usage notes:

* Can be used as a regular filesystem, but should not (high latency)
* Files are cached in memory. If you write or read very large files this may cause issues (you are limited by available ram)
* Neither permissions or timestamps are supported by B2. B2_fuse ignores any requests to set permissions.
* Filesystem contains ".sha1" files, these are undeletable and contain the hash of the file without the postfix. This feature can be disabled by setting variable "enable_hashfiles" to False.
* Having many files in a bucket (multiples of 1000) will drastically increase the startup time/mount time. 
* For optimal performance and throughput, you should store a few large files. Small files suffer from latency issues due to the way B2 API is implemented. Large files will allow you to saturate your internet connection.

### With local copy

If you wish to try the local copy variant, this requires a folder to store the local replica in addition to the mountpoint you wish to use. 

```
python b2local.py <local_directory> <mountpoint>
```

Usage notes:

* Threaded uploads and deletions, large files will cause problems as splitting uploads is not yet supported
* Changes will only take effect online after 15+ seconds. This is a timeout in order to handle multiple successive closures and flushing of the same file. 
* First synchronization will take a long time. Threading speeds up the process, but FUSE driver will not respond to filesystem requests before synchronization is complete.

### Application specific notes:

####Using RSync with B2 Fuse

Since there is no support for updating file times or permissions in a bucket, rsync must be told to ignore both when synching folders (sync will be based on checksum meaning files have to be downloaded to compare).

```
rsync -avzh --no-perms --no-owner --no-group dir1/ dir2/ 
```

Option "--inplace" may also be useful. RSync creates a temporary file when syncing, this option will make RSync update the file inplace instead (Effectively twice as fast syncing).

####Using unison to synchronize against mounted folder

Again, we ignore permissions as these are not applicable.

```
unison dir1/ dir2/ -auto  -perms 0  -batch
```

#### Using encfs to overlay a locally encrypted filesystem onto the bucket

Install encfs (apt-get install encfs)

```
encfs <bucket_mountpoint> <encrypted_filesystem>
```


### Known issues:

* Concurrent access from multiple client will lead to inconsistent results
* Small files give low read/write performance (due to high latency)



License: MIT license


