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

License: MIT license


#### Known issues:

* Concurrent access from multiple client will lead to inconsistent results
* RSync does not work (lacking unknown file primitiv)
* Directories are not supported
