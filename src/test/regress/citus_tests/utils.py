import subprocess
import os


USER = "postgres"


def psql(pg_path, port, command):
    return subprocess.run(
        [
            os.path.join(pg_path, "psql"),
            "-U",
            USER,
            "-p",
            str(port),
            "-c",
            command,
            "-P",
            "pager=off",
            "--no-psqlrc",
        ],
        check=True,
    )


def psql_capture(pg_path, port, command):
    return subprocess.check_output(
        [
            os.path.join(pg_path, "psql"),
            "-U",
            USER,
            "-p",
            str(port),
            "-c",
            command,
            "-P",
            "pager=off",
            "--no-psqlrc",
            "--tuples-only",
        ],
    )


# Taken from https://stackoverflow.com/questions/431684/how-do-i-change-directory-cd-in-python/13197763#13197763


class cd(object):
    """Context manager for changing the current working directory"""

    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        print(self.savedPath)
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
