
### Permanent mount points

#### Setup
1. put contents of b2_fuse-master in /etc/b2fuse
2. create the config.yaml mentioned in the github documenation in /etc/b2fuse
3. create /mnt/b2fuse-mount & make it owned by the admin account this is being done under and then add it to the group www-data
4. chown 775 b2fuse-mount 

#### Start as a Service
NOTE: Below are a few ways this can be done. 
The guide below is only complete when utilizing upstart in Ubuntu, though links for reference to other methods are included.

##### Ubuntu with upstart
This has been seen to work in Ubuntu 14.04 LTS as well as 16.04 LTS
* create /etc/init/b2fuse.conf (this will allow the b2fuse mount to start when the system starts...)
* In b2fuse.conf put the following lines
```
start on filesystem
exec python /etc/b2fuse/b2fuse.py /mnt/b2fuse-mount
```
##### Ubuntu without upstart
* There are many options listed here: http://stackoverflow.com/questions/24518522/run-python-script-at-startup-in-ubuntu

##### Red Hat/CentOS
* Follow the instructions and make sure the options you choose make the command run when the OS starts
* http://www.abhigupta.com/2010/06/how-to-auto-start-services-on-boot-in-centos-redhat/

##### Any other Linux versions that don't work like the above 
* Make sure to find instructions and follow them to start the python script when the OS starts under an appropriate run level.
NOTE: Mounting the fuse drive as root may cause problems (which is a problem that may occur if attempting to use roots crontab).


### Using fstab (advanced)

Using fstab requires the use of an intermediate script. An example is found in _start_b2fuse-py.sh_

The script can not be renamed unless the "name_of_this_script_must_be" variable be changed accordingly.

The script can be symlinked to another place, for example somewhere in the $PATH with another convenient name.

The configuration file must be named "config_&lt;bucket_name&gt;_bucket.yaml".

Tha configuration file may optionally have a line with the format: "mount_point: &lt;path_to_mount_point&gt;" with no spaces on it.

If the mount point line doesn't exist in the configuration file the script will ask to write the mount point or it can be specified in the command line. If the mount mount point is specified in the command line it will override the mount point in the configuration file if such line exists.

The script can be run in a fully non interactive way with this command:

echo "n" | start_b2fuse-py.sh &lt;bucket_name\> &lt;mount_point&gt; [--enable_hashfiles] [--use_disk] ["--temp_folder &lt;path_to_temp_folder&gt;"]

Instead of "start_b2fuse-py.sh" we can invoke any symlink that points to it. If the symlink is in the $PATH it is not necessary to especify its full path. The symlink to the script can have whatever name we choose.

The "start_b2fuse-py.sh" script must always stay in the installation folder. Use symlinks to access it from other places.

The last three parameters are optional and ["--temp_folder &lt;path_to_temp_folder&gt;"] must be inside quotation marks.
