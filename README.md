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
* Partial files are cached in memory. If you write or read very large files this may cause issues. Todo add memory usage limit.
* Neither permissions or timestamps is supported by B2. B2_fuse should gracefully ignore any requests to set permissions.
* Filesystem contains ".sha1" files, these are undeletable and contain the hash of the file without the postfix. This feature can be disabled by setting variable "enable_hashfiles" to False.

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

License: MIT license


