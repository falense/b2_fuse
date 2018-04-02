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


class Directory(object):
    def __init__(self, name):
        self._name = name
        self._content = []
        self._directories = {}

    def __len__(self):
        return len(self._directories)

    def __getitem__(self, i):
        return self._directories[i]

    def get_directory(self, name):
        return self._directories.get(name)

    def get_directories(self):
        return self._directories.values()

    def add_directory(self, name):
        self._directories[name] = Directory(name)

    def add_file(self, file_info):
        self._content.append(file_info)

    def get_file_info(self, name):
        file_info = filter(lambda f: str(f['fileName']) == name, self._content)

        if len(file_info) == 1:
            return file_info[0]
        else:
            return None

    def get_file_infos(self):
        return self._content

    def __repr__(self):
        return self._name

    def get_content_names(self):
        files = [file_info['fileName'] for file_info in self._content]
        directories = map(str, self._directories)

        return directories + files


class DirectoryStructure(object):
    def __init__(self):
        self._directories = Directory("")

    def update_structure(self, file_info_list, local_directories):
        self._directories = Directory("")

        local_directories_split = map(lambda f: f.split("/"), local_directories)
        for directory in local_directories_split:
            self._lookup(self._directories, directory, True)

        online_directories_split = map(
            lambda file_info: file_info['fileName'].split("/")[:-1], file_info_list
        )
        for directory in online_directories_split:
            self._lookup(self._directories, directory, True)

        for file_info in file_info_list:
            folder_path_split = file_info['fileName'].split("/")[:-1]
            folder_path = "/".join(folder_path_split)
            directory = self.get_directory(folder_path)
            directory.add_file(file_info)

    def _lookup(self, directory, path, update=False):
        if len(path) == 0:
            return directory

        head = path.pop(0)
        if update and directory.get_directory(head) is None:
            directory.add_directory(head)

        if directory.get_directory(head) is not None:
            return self._lookup(directory.get_directory(head), path, update)
        else:
            return None

    def is_directory(self, path):
        return self.get_directories(path) is not None

    def is_file(self, path):
        return self.get_file_info(path) is not None

    def get_directories(self, path):
        if len(path) == 0:
            return self._directories.get_directories()
        else:
            path_split = path.split("/")
            r = self._lookup(self._directories, path_split)

            if r is not None:
                return r.get_directories()
            else:
                return None

    def get_directory(self, path):
        if len(path) == 0:
            return self._directories
        else:
            path_split = path.split("/")
            r = self._lookup(self._directories, path_split)

            return r

    def get_file_info(self, path):
        path_split = path.split("/")

        filename = path_split[-1]
        file_path = path_split[:-1]

        directory = self._lookup(self._directories, file_path)

        if directory is not None:
            return directory.get_file_info(path)
        else:
            return None
