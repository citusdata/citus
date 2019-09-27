#!/usr/bin/env python3

import fnmatch
import os
import subprocess
import sys


specs_path = sys.argv[1]

MX_COMMON_FILE = os.path.join(specs_path, 'isolation_mx_common.spec')

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

for file in os.listdir(specs_path):
    if fnmatch.fnmatch(file, '[!i]*_mx.spec'):
        abs_dst_file_path = os.path.join(specs_path, remove_prefix(file, 'base_'))
        abs_src_file_path = os.path.join(specs_path, file)
        with open(abs_dst_file_path, '+w') as dest:
            subprocess.call(['cat', MX_COMMON_FILE], stdout=dest)
            subprocess.call(['cat', abs_src_file_path], stdout=dest)
