# b2_fuse - FUSE for Backblaze B2
 
### Version: 1.0

#### Warning this software may contain bugs, be careful of using it with important data.

### Basic setup:

Requires FUSE for python to work

Usage:

```
python b2_fuse.py <mountpoint>
```

Config file example ("config.yaml"):

```
accountId: <youraccountid>
applicationKey: <yourapplicationid>
bucketId: <yourbucketid>
```

### Usage notes:

* Can be used as a regular filesystem, but should not (as it is actually in the cloud, no local copy exists)
* Partial files are cached in memory. If you write or read very large files this may cause issues (you are limited by available ram)
* Neither permissions or timestamps are supported by B2. B2_fuse should gracefully ignore any requests to set permissions.
* Filesystem contains ".sha1" files, these are undeletable and contain the hash of the file without the postfix. This feature can be disabled by setting variable "enable_hashfiles" to False.
* Maximum files per bucket is limited to 1000. This limit is given by the way B2 API is structured, it can be increased at the cost of performance. Open an issue if this is a problem for your use-cases.
* For optimal performance and throughput, you should store a few large files. Small files suffer from latency issues due to the way B2 API is implemented. Large files will allow you to saturate your internet connection.

### Application specific notes:

####Using RSync with B2 Fuse

Since there is no support for updating file times or permissions in a bucket, rsync must be told to ignore both when synching folders (sync will be based on checksum meaning files have to be downloaded to compare).

```
rsync -avzh --no-perms --no-owner --no-group dir1/ dir2/ 
```

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
* Maximum file count per bucket is 1000
* Small files give low read/write performance (due to high latency)

License: MIT license


