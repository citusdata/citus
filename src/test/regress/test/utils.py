from pathlib import Path
import subprocess
from contextlib import contextmanager, closing
from contextlib import asynccontextmanager

import psycopg
import os
import re
import sys
import time
import asyncio
import socket
from tempfile import gettempdir
import filelock
import typing
import platform


ENABLE_VALGRIND = bool(os.environ.get("ENABLE_VALGRIND"))
USE_SUDO = bool(os.environ.get("USE_SUDO"))


LINUX = False
MACOS = False
FREEBSD = False
OPENBSD = False

if platform.system() == "Linux":
    LINUX = True
elif platform.system() == "Darwin":
    MACOS = True
elif platform.system() == "FreeBSD":
    FREEBSD = True
elif platform.system() == "OpenBSD":
    OPENBSD = True

BSD = MACOS or FREEBSD or OPENBSD


def eprint(*args, **kwargs):
    """eprint prints to stderr"""

    print(*args, file=sys.stderr, **kwargs)


def run(command, *args, check=True, shell=True, silent=False, **kwargs):
    """run runs the given command and prints it to stderr"""

    if not silent:
        eprint(f"+ {command} ")
    if silent:
        kwargs.setdefault("stdout", subprocess.DEVNULL)
    return subprocess.run(command, *args, check=check, shell=shell, **kwargs)


def sudo(command, *args, shell=True, **kwargs):
    """
    A version of run that prefixes the command with sudo when the process is
    not already run as root
    """
    effective_user_id = os.geteuid()
    if effective_user_id == 0:
        return run(command, *args, shell=shell, **kwargs)
    if shell:
        return run(f"sudo {command}", *args, shell=shell, **kwargs)
    else:
        return run(["sudo", *command])


def get_pg_major_version():
    full_version_string = run(
        "initdb --version", stdout=subprocess.PIPE, encoding="utf-8", silent=True
    ).stdout
    major_version_string = re.search("[0-9]+", full_version_string)
    assert major_version_string is not None
    return int(major_version_string.group(0))


PG_MAJOR_VERSION = get_pg_major_version()

# this is out of ephemeral port range for many systems hence
# it is a lower change that it will conflict with "in-use" ports
PORT_LOWER_BOUND = 10200

# ephemeral port start on many Linux systems
PORT_UPPER_BOUND = 32768

next_port = PORT_LOWER_BOUND


class PortLock:
    def __init__(self):
        global next_port
        while True:
            next_port += 1
            if next_port >= PORT_UPPER_BOUND:
                next_port = PORT_LOWER_BOUND

            self.lock = filelock.FileLock(Path(gettempdir()) / f"port-{next_port}.lock")
            try:
                self.lock.acquire(timeout=0)
            except filelock.Timeout:
                continue

            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                try:
                    s.bind(("127.0.0.1", next_port))
                    self.port = next_port
                    break
                except Exception:
                    continue

    def release(self):
        self.lock.release()


class QueryRunner:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.default_db = "postgres"
        self.default_user = "postgres"

    def set_default_connection_options(self, options):
        options.setdefault("dbname", self.default_db)
        options.setdefault("user", self.default_user)
        if ENABLE_VALGRIND:
            # If valgrind is enabled PgBouncer is a significantly slower to
            # respond to connection requests, so we wait a little longer.
            options.setdefault("connect_timeout", 20)
        else:
            options.setdefault("connect_timeout", 3)
        # needed for Ubuntu 18.04
        options.setdefault("client_encoding", "UTF8")

    def conn(self, *, autocommit=True, **kwargs):
        """Open a psycopg connection to this server"""
        self.set_default_connection_options(kwargs)
        return psycopg.connect(
            autocommit=autocommit,
            host=self.host,
            port=self.port,
            **kwargs,
        )

    def aconn(self, *, autocommit=True, **kwargs):
        """Open an asynchronous psycopg connection to this server"""
        self.set_default_connection_options(kwargs)
        return psycopg.AsyncConnection.connect(
            autocommit=autocommit,
            host=self.host,
            port=self.port,
            **kwargs,
        )

    @contextmanager
    def cur(self, autocommit=True, **kwargs):
        """Open an psycopg cursor to this server

        The connection and the cursors automatically close once you leave the
        "with" block
        """
        with self.conn(
            autocommit=autocommit,
            **kwargs,
        ) as conn:
            with conn.cursor() as cur:
                yield cur

    @asynccontextmanager
    async def acur(self, **kwargs):
        """Open an asynchronous psycopg cursor to this server

        The connection and the cursors automatically close once you leave the
        "async with" block
        """
        async with await self.aconn(**kwargs) as conn:
            async with conn.cursor() as cur:
                yield cur

    def sql(self, query, params=None, **kwargs):
        """Run an SQL query

        This opens a new connection and closes it once the query is done
        """
        with self.cur(**kwargs) as cur:
            cur.execute(query, params=params)

    def sql_value(self, query, params=None, **kwargs):
        """Run an SQL query that returns a single cell and return this value

        This opens a new connection and closes it once the query is done
        """
        with self.cur(**kwargs) as cur:
            cur.execute(query, params=params)
            result = cur.fetchall()
            assert len(result) == 1
            assert len(result[0]) == 1
            value = result[0][0]
            return value

    def asql(self, query, **kwargs):
        """Run an SQL query in asynchronous task

        This opens a new connection and closes it once the query is done
        """
        return asyncio.ensure_future(self.asql_coroutine(query, **kwargs))

    async def asql_coroutine(
        self, query, params=None, **kwargs
    ) -> typing.Optional[typing.List[typing.Any]]:
        async with self.acur(**kwargs) as cur:
            await cur.execute(query, params=params)
            try:
                return await cur.fetchall()
            except psycopg.ProgrammingError as e:
                if "the last operation didn't produce a result" == str(e):
                    return None
                raise

    def psql(self, query, **kwargs):
        """Run an SQL query using psql instead of psycopg

        This opens a new connection and closes it once the query is done
        """

        self.set_default_connection_options(kwargs)
        connect_options = " ".join([f"{k}={v}" for k, v in kwargs.items()])

        run(
            ["psql", f"port={self.port} {connect_options}", "-c", query],
            shell=False,
            silent=True,
        )

    @contextmanager
    def transaction(self, **kwargs):
        with self.cur(**kwargs) as cur:
            with cur.connection.transaction():
                yield cur

    def sleep(self, duration=3, **kwargs):
        """Run pg_sleep"""
        return self.sql(f"select pg_sleep({duration})", **kwargs)

    def asleep(self, duration=3, times=1, sequentially=False, **kwargs):
        """Run pg_sleep asynchronously in a task.

        times:
            You can create a single task that opens multiple connections, which
            run pg_sleep concurrently. The asynchronous task will only complete
            once all these pg_sleep calls are finished.
        sequentially:
            Instead of running all pg_sleep calls spawned by providing
            times > 1 concurrently, this will run them sequentially.
        """
        return asyncio.ensure_future(
            self.asleep_coroutine(
                duration=duration, times=times, sequentially=sequentially, **kwargs
            )
        )

    async def asleep_coroutine(self, duration=3, times=1, sequentially=False, **kwargs):
        """This is the coroutine that the asleep task runs internally"""
        if not sequentially:
            await asyncio.gather(
                *[
                    self.asql(f"select pg_sleep({duration})", **kwargs)
                    for _ in range(times)
                ]
            )
        else:
            for _ in range(times):
                await self.asql(f"select pg_sleep({duration})", **kwargs)

    def test(self, **kwargs):
        """Test if you can connect"""
        return self.sql("select 1", **kwargs)

    def atest(self, **kwargs):
        """Test if you can connect asynchronously"""
        return self.asql("select 1", **kwargs)

    def psql_test(self, **kwargs):
        """Test if you can connect with psql instead of psycopg"""
        return self.psql("select 1", **kwargs)

    @contextmanager
    def enable_firewall(self):
        """Enables the firewall for the platform that you are running

        Normally this should not be called directly, and instead drop_traffic
        or reject_traffic should be used.
        """
        fw_token = None
        if BSD:
            if MACOS:
                command_stderr = sudo(
                    f"pfctl -E", stderr=subprocess.PIPE, text=True
                ).stderr
                match = re.search(r"^Token : (\d+)", command_stderr, flags=re.MULTILINE)
                assert match is not None
                fw_token = match.group(1)
            sudo(
                'bash -c "'
                f"echo 'anchor \\\"port_{self.port}\\\"'"
                f' | pfctl -a pgbouncer_test -f -"'
            )
        try:
            yield
        finally:
            if MACOS:
                sudo(f"pfctl -X {fw_token}")

    @contextmanager
    def drop_traffic(self):
        """Drops all TCP packets to this query runner"""
        with self.enable_firewall():
            if LINUX:
                sudo(
                    "iptables --append OUTPUT "
                    "--protocol tcp "
                    f"--destination {self.host} "
                    f"--destination-port {self.port} "
                    "--jump DROP "
                )
            elif BSD:
                sudo(
                    "bash -c '"
                    f'echo "block drop out proto tcp from any to {self.host} port {self.port}"'
                    f"| pfctl -a pgbouncer_test/port_{self.port} -f -'"
                )
            else:
                raise Exception("This OS cannot run this test")
            try:
                yield
            finally:
                if LINUX:
                    sudo(
                        "iptables --delete OUTPUT "
                        "--protocol tcp "
                        f"--destination {self.host} "
                        f"--destination-port {self.port} "
                        "--jump DROP "
                    )
                elif BSD:
                    sudo(f"pfctl -a pgbouncer_test/port_{self.port} -F all")

    @contextmanager
    def reject_traffic(self):
        """Rejects all traffic to this query runner with a TCP RST message"""
        with self.enable_firewall():
            if LINUX:
                sudo(
                    "iptables --append OUTPUT "
                    "--protocol tcp "
                    f"--destination {self.host} "
                    f"--destination-port {self.port} "
                    "--jump REJECT "
                    "--reject-with tcp-reset"
                )
            elif BSD:
                sudo(
                    "bash -c '"
                    f'echo "block return-rst out out proto tcp from any to {self.host} port {self.port}"'
                    f"| pfctl -a pgbouncer_test/port_{self.port} -f -'"
                )
            else:
                raise Exception("This OS cannot run this test")
            try:
                yield
            finally:
                if LINUX:
                    sudo(
                        "iptables --delete OUTPUT "
                        "--protocol tcp "
                        f"--destination {self.host} "
                        f"--destination-port {self.port} "
                        "--jump REJECT "
                        "--reject-with tcp-reset"
                    )
                elif BSD:
                    sudo(f"pfctl -a pgbouncer_test/port_{self.port} -F all")


class Postgres(QueryRunner):
    def __init__(self, pgdata):
        self.port_lock = PortLock()
        super().__init__("127.0.0.1", self.port_lock.port)
        self.pgdata = pgdata
        self.log_path = self.pgdata / "pg.log"
        self.connections = {}
        self.cursors = {}

    def initdb(self):
        run(
            f"initdb -A trust --nosync --username postgres --pgdata {self.pgdata}",
            stdout=subprocess.DEVNULL,
        )

        with self.conf_path.open(mode="a") as pgconf:
            pgconf.write("unix_socket_directories = '/tmp'\n")
            pgconf.write("log_connections = on\n")
            pgconf.write("log_disconnections = on\n")
            pgconf.write("logging_collector = off\n")
            pgconf.write("shared_preload_libraries = 'citus'\n")
            # We need to make the log go to stderr so that the tests can
            # check what is being logged.  This should be the default, but
            # some packagings change the default configuration.
            pgconf.write("log_destination = stderr\n")
            # This makes tests run faster and we don't care about crash safety
            # of our test data.
            pgconf.write("fsync = false\n")

    def pgctl(self, command, **kwargs):
        run(f"pg_ctl -w --pgdata {self.pgdata} {command}", **kwargs)

    def apgctl(self, command, **kwargs):
        return asyncio.create_subprocess_shell(
            f"pg_ctl -w --pgdata {self.pgdata} {command}", **kwargs
        )

    def start(self):
        try:
            self.pgctl(f'-o "-p {self.port}" -l {self.log_path} start')
        except Exception:
            print("\n\nPG_LOG\n")
            with self.log_path.open() as f:
                print(f.read())
            raise

    def stop(self, mode="fast"):
        self.pgctl(f"-m {mode} stop", check=False)

    def cleanup(self):
        self.stop()
        self.port_lock.release()

    def restart(self):
        self.stop()
        self.start()

    def reload(self):
        self.pgctl("reload")
        time.sleep(1)

    async def arestart(self):
        process = await self.apgctl("-m fast restart")
        await process.communicate()

    def nossl_access(self, dbname, auth_type):
        """Prepends a local non-SSL access to the HBA file"""
        with self.hba_path.open() as pghba:
            old_contents = pghba.read()
        with self.hba_path.open(mode="w") as pghba:
            pghba.write(f"local      {dbname}   all                {auth_type}\n")
            pghba.write(f"hostnossl  {dbname}   all  127.0.0.1/32  {auth_type}\n")
            pghba.write(f"hostnossl  {dbname}   all  ::1/128       {auth_type}\n")
            pghba.write(old_contents)

    def ssl_access(self, dbname, auth_type):
        """Prepends a local SSL access rule to the HBA file"""
        with self.hba_path.open() as pghba:
            old_contents = pghba.read()
        with self.hba_path.open(mode="w") as pghba:
            pghba.write(f"hostssl  {dbname}   all  127.0.0.1/32  {auth_type}\n")
            pghba.write(f"hostssl  {dbname}   all  ::1/128       {auth_type}\n")
            pghba.write(old_contents)

    @property
    def hba_path(self):
        return self.pgdata / "pg_hba.conf"

    @property
    def conf_path(self):
        return self.pgdata / "postgresql.conf"

    def commit_hba(self):
        """Mark the current HBA contents as non-resetable by reset_hba"""
        with self.hba_path.open() as pghba:
            old_contents = pghba.read()
        with self.hba_path.open(mode="w") as pghba:
            pghba.write("# committed-rules\n")
            pghba.write(old_contents)

    def reset_hba(self):
        """Remove any HBA rules that were added after the last call to commit_hba"""
        with self.hba_path.open() as f:
            hba_contents = f.read()
        committed = hba_contents[hba_contents.find("# committed-rules\n") :]
        with self.hba_path.open("w") as f:
            f.write(committed)

    async def delayed_start(self, delay=1):
        """Start Postgres after a delay

        NOTE: The sleep is asynchronous, but while waiting for Postgres to
        start the pg_ctl start command will block the event loop. This is
        currently acceptable for our usage of this method in the existing
        tests and this way it was easiest to implement. However, it seems
        totally reasonable to change this behaviour in the future if necessary.
        """
        await asyncio.sleep(delay)
        self.start()

    def configure(self, config):
        """Configure specific Postgres settings using ALTER SYSTEM SET

        NOTE: after configuring a call to reload or restart is needed for the
        settings to become effective.
        """
        self.sql(f"alter system set {config}")
