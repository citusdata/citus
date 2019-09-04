import subprocess
import os
from config import USER


def run(command):
    return subprocess.call(command, shell=True)


def psql(pg_path, port, command):
    run('{}/psql -U {} -p {} -c "{}"'.format(pg_path, USER,  port, command))

# Taken from https://stackoverflow.com/questions/431684/how-do-i-change-directory-cd-in-python/13197763#13197763
class cd(object):
    """Context manager for changing the current working directory"""

    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
