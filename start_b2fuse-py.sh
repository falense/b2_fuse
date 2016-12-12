#!/bin/bash

# Version of this script
script_version="1.1"
echo
echo "     Bash script to invoke b2fuse.py"
echo "           The vesion of this script is "$script_version""

# Help screen
echo "==============================================================="
echo "             -----USAGE-----"
echo
echo "$(tput bold)Prerequisite$(tput sgr0): the configuration file with the accountId, applicationKey, bucketId and,"
echo "optionally, mount_point must be named '$(tput bold)config_<bucket_name>_bucket.yaml$(tput sgr0)'"
echo "The optional mount point line must be: \"mount_point: <path_to_mount_point>\""
echo
echo "$(tput bold) "$(basename -- "$0")" $(tput sgr0)"
echo "        # Interactive mode. Mount point taken from config file 'config_<bucket_name>_bucket.yaml'"
echo "          if it was added to the file. Otherwise, it will ask the mount point."
echo
echo "$(tput bold) "$(basename -- "$0")" $(tput sgr0) <bucket_name>"
echo "        # The same as before but specifying the bucket in the command line."
echo
echo "$(tput bold) "$(basename -- "$0")" $(tput sgr0) <bucket_name> <mount_point>"
echo "        # Specifying the bucket and mount point in the command line. If the mount point is"
echo "          configured in the 'config_<bucket_name>_bucket.yaml' file this command line overrides it."
echo
echo "  # In the above cases it will ask if additional optional parameters are to be added:"
echo "    --enable_hashfiles --use_disk --temp_folder"
echo
echo "echo \"n\" |$(tput bold) "$(basename -- "$0")" $(tput sgr0) <bucket_name> <mount_point> [--enable_hashfiles] [--use_disk] [\"--temp_folder <path_to_temp_folder>\"]"
echo "  # Fully non interactive mode intended to be used in other scripts or programs"
echo "    and in configuration files that run programs at startup like \$HOME/.bashrc"
echo "    The last three parameters are optional. Beware of \"\" in \"--temp_folder <path_to_temp_folder>\"."
echo "    <path_to_temp_folder> must not contain any space."
echo
echo "#The script in the installation folder can be symlinked to"
echo " another name somewhere in the \$PATH for convenience."
echo "==============================================================="

# Directory of the script
script_dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
pruned_dir_name=${script_dir/*b2_fuse-/}

# Version of the program inside b2fuse.py if any.
version_line_in_b2fusepy="$(grep "parser.add_argument('--version',action='version', version=" "$script_dir"/b2fuse.py |tail --bytes 22 | head --bytes 20)"

if [ -n "$version_line_in_b2fusepy" ]
then
	echo "Vesion of b2fuse.py as reported by the program:"
	echo "         $version_line_in_b2fusepy"
fi

# Version of the program deduced from the folder it is located in.
if [ "$pruned_dir_name" = "$script_dir" ]
then
	echo "The folder containing the program is "$script_dir"; it looks like"
	echo "      b2_fuse branch/version is:   \"master\""
else
	echo
	echo "According to the name of the folder it is installed in,"
	echo "      b2_fuse version is:   "$pruned_dir_name""
fi

# name of the script
name_of_this_script_must_be=start_b2fuse-py.sh
name_of_this_script="$(basename "$(test -L "$0" && readlink "$0" || echo "$0")")"

if [ "$name_of_this_script" != "$name_of_this_script_must_be" ]
then
	echo
	echo "Error:"
	echo "This script must not be renamed. Its name must be  "$name_of_this_script_must_be""
	echo "Aborting."
	echo
	exit
fi

# Warn that it is better to run this as a normal user
run_by_root_or_not="$(whoami)"
if [ "$run_by_root_or_not" = "root" ]
then
	echo
	echo "It is better not mount it as root."
	echo "Press <Enter> to continue or abort with <Ctrl-c>"
	read not_to_be_used_variable
fi

# Get the name of the bucket from command line or ask for it:
if [ -n "$1" ]
then
	bucket_name="$1"
else
	echo
	echo "Te buckets configured in this folder ("$script_dir") are:"
	list_of_config_files="$(ls --quoting-style=shell-always "$script_dir"/config_*_bucket.yaml)"
	trim_to_get_bucket_names=${list_of_config_files//"$script_dir"\/config_/}
	echo ${trim_to_get_bucket_names//_bucket.yaml/}
	echo
	echo "What's the name of the bucket you want to mount?"
	read bucket_name
fi

# Check that the configuration file exist
config_name=""$script_dir"/config_"$bucket_name"_bucket.yaml"
if [ ! -e "$config_name" ]
then
	echo
	echo "ERROR:"
	echo "The configuration file for the bucket \""$bucket_name"\" doesn't exist or bucket \""$bucket_name"\" is misspelled"
	echo "Aborting."
	echo
	exit
fi

# Set the mount point
if [ -n "$2" ]
then
	echo
	echo "Mount point is \""$2"\" as specified in the command line."
	echo
	mount_point="$2"
else
		mount_point_from_config_file="$(grep mount_point -s "$config_name" | tail --bytes +14)"
		if [ -n "$mount_point_from_config_file" ]
		then
			echo
			echo "As specified in "$config_name", mount point is:"
			echo
			echo "           \""$mount_point_from_config_file"\""
			echo
			mount_point="$mount_point_from_config_file"
		else
				if [ -z "$2" ] && [ -z "$mount_point_from_config_file" ]
				then
					echo
					echo "No mount point in the command line and"
					echo "no mount point configured in "$config_name", therefore:"
					echo
					echo "What mount point do you want for the \""$bucket_name"\" bucket?"
					read mount_point
				fi
		fi
fi

# Check if mount point exist
if [ ! -e "$mount_point" ]
then
	echo
	echo "ERROR:"
	echo "The mount point \""$mount_point"\" does not exist!"
	echo "Aborting"
	echo
	exit
fi

# Check if the mount point is already used by a previous mount
if [ -n "$(grep " $mount_point " /etc/mtab)" ]
then
	echo
	echo "The folder \""$mount_point"\" is already mounted. Manually unmount it first."
	echo "If it is mounted with a FUSE mount you can unmount it as a normal user with:"
	echo
	echo "      fusermount -u \""$mount_point"\""
	echo
	exit
fi

# Add optional parameters
echo "Optional parameters: --enable_hashfiles --use_disk --temp_folder"
echo "Add any optional parameter? (y/n)"
read -n1 want_optional_parameters
if [[ $want_optional_parameters = [Yy] ]]
then
	echo
	echo "Add \"--enable_hashfiles\"? (y/n)"
	read -n1 hash_option
	echo
	echo "Add \"--use_disk\"? (y/n)"
	read -n1 disk_option
	echo
	echo "Add \"--temp_folder\"? (y/n)"
	read -n1 temp_option

	if [[ "$hash_option" = [Yy] ]]
	then	additional_parameters="--enable_hashfiles"
	fi

	if [[ "$disk_option" = [Yy] ]]
	then	additional_parameters=""$additional_parameters" --use_disk"
	fi

	if [[ "$temp_option" = [Yy] ]]
	then
		echo
		echo "         Write the full path of the temp folder"
		read optional_temp_folder
		additional_parameters=""$additional_parameters" --temp_folder "$optional_temp_folder""
	fi
else
	echo
	echo "No optional parameters will be added."
fi

echo
echo "additional parameters that will be added:   \""$additional_parameters"\""
echo

# Run the program
exec python "$script_dir"/b2fuse.py --config_filename "$config_name" $3 $4 $5 $additional_parameters "$mount_point" &

if [[ $? = 0 ]]
then
	echo
	echo "To unmount this bucket use this command as a normal user:"
	echo "        fusermount -u \""$mount_point"\""
	echo
fi
exit 0
