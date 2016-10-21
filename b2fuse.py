#!/usr/bin/env python
# -*- coding: utf-8 -*-

#The MIT License (MIT)

#Copyright (c) 2015 Sondre Engebraaten

#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import argparse
import logging
import yaml

from fuse import FUSE

from b2fuse_main import B2Fuse


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint for the B2 bucket")

    parser.add_argument('--enable_hashfiles', dest='enable_hashfiles', action='store_true')
    parser.set_defaults(enable_hashfiles=False)

    parser.add_argument('--use_disk', dest='use_disk', action='store_true')
    parser.set_defaults(use_disk=False)

    parser.add_argument(
        "--account_id",
        type=str,
        default=None,
        help="Account ID for your B2 account (overrides config)"
    )
    parser.add_argument(
        "--application_key",
        type=str,
        default=None,
        help="Application key for your account  (overrides config)"
    )
    parser.add_argument(
        "--bucket_id",
        type=str,
        default=None,
        help="Bucket ID for the bucket to mount (overrides config)"
    )

    parser.add_argument("--memory_limit", type=int, default=128, help="Memory limit")
    parser.add_argument("--temp_folder", type=str, default=".tmp/", help="Temporary file folder")
    parser.add_argument("--config_filename", type=str, default="config.yaml", help="Config file")

    return parser


def load_config(config_filename):
    with open(config_filename) as f:
        return yaml.load(f.read())


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s:%(levelname)s:%(message)s")

    parser = create_parser()
    args = parser.parse_args()

    if args.config_filename:
        config = load_config(args.config_filename)
    else:
        config = {}

    if args.account_id:
        config["accountId"] = args.account_id

    if args.application_key:
        config["applicationKey"] = args.application_key

    if args.bucket_id:
        config["bucketId"] = args.bucket_id

    if args.enable_hashfiles:
        config["enableHashfiles"] = args.enable_hashfiles
    else:
        config["enableHashfiles"] = False

    if args.memory_limit:
        config["memoryLimit"] = args.memory_limit

    if args.temp_folder:
        config["tempFolder"] = args.temp_folder

    if args.use_disk:
        config["useDisk"] = args.use_disk
    else:
        config["useDisk"] = False

    with B2Fuse(
        config["accountId"], config["applicationKey"], config["bucketId"],
        config["enableHashfiles"], config["memoryLimit"], config["tempFolder"], config["useDisk"]
    ) as filesystem:
        FUSE(filesystem, args.mountpoint, nothreads=True, foreground=True)
