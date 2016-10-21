#!/bin/bash -u

base_remote="${1:-origin}"
base_branch="${2:-master}"
base_remote_branch="${3:-master}"

if ! type yapf &> /dev/null
then
    echo "Please install yapf, then try again."
    exit 1
fi

if [ "$(git rev-parse ${base_branch})" != "$(git rev-parse ${base_remote}/${base_remote_branch})" ]; then
    echo """running yapf in full mode, because an assumption that master and origin/master are the same, is broken. To fix it, do this:
git checkout master
git pull --ff-only

then checkout your topic branch and run $0.
If the base branch on github is not called 'origin', invoke as $0 proper_origin_remote_name. Then your remote needs to be synched with your master too.
"""
    yapf --in-place --recursive .
else
    echo 'running yapf in incremental mode'
    head=`mktemp`
    master=`mktemp`
    git rev-list --first-parent HEAD > "$head"  # list of commits being a history of HEAD branch, but without commits merged from master after forking
    git rev-list origin/master > "$master"  # list of all commits on history of master

    changed_files=`git diff --name-only "$(git rev-parse --abbrev-ref HEAD)..${base_remote}/${base_remote_branch}"`
    dirty_files=`git ls-files -m`
    files_to_check="$((echo "$changed_files"; echo "$dirty_files") | grep '\.py$' | sort -u)"
    if [ -z "$files_to_check" ]; then
        echo 'nothing to run yapf on after all'
    else
        echo -n 'running yapf... '
        echo "$files_to_check" | (while read file
        do
            if [ -e "$file" ]; then
                # in case file was added since master, but then was removed
                yapf --in-place "$file" &
            fi
        done
        wait
        )

        echo 'done'
    fi
fi

pyflakes .
echo 'ok'
