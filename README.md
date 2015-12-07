# b2_fuse - FUSE for Backblaze B2
 
### Version: 0.1

#### Warning this software is BETA. It may destroy your bucket, you have been warned.

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

License: MIT license



#### Known issues:

* Concurrent access from multiple client will lead to inconsistent results
* Directories are not supported
