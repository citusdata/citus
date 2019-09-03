import subprocess

def run(command):
    subprocess.call(command, shell = True)


def psql(pg_path, port, command):
    run('{}/psql -p {} -c "{}"'.format(pg_path, port, command))