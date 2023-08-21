import asyncio
import atexit
import concurrent.futures
import os
import pathlib
import platform
import random
import re
import shutil
import socket
import subprocess
import sys
import time
import typing
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, closing, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import gettempdir

import filelock
import psycopg
import psycopg.sql
import utils
from psycopg import sql
from utils import USER

# This SQL returns true ( 't' ) if the Citus version >= 11.0.
IS_CITUS_VERSION_11_SQL = "SELECT (split_part(extversion, '.', 1)::int >= 11) as is_11 FROM pg_extension WHERE extname = 'citus';"

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

TIMEOUT_DEFAULT = timedelta(seconds=int(os.getenv("PG_TEST_TIMEOUT_DEFAULT", "10")))
FORCE_PORTS = os.getenv("PG_FORCE_PORTS", "NO").lower() not in ("no", "0", "n", "")

REGRESS_DIR = pathlib.Path(os.path.realpath(__file__)).parent.parent
REPO_ROOT = REGRESS_DIR.parent.parent.parent
CI = os.environ.get("CI") == "true"


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


def capture(command, *args, **kwargs):
    """runs the given command and returns its output as a string"""
    return run(command, *args, stdout=subprocess.PIPE, text=True, **kwargs).stdout


PG_CONFIG = os.environ.get("PG_CONFIG", "pg_config")
PG_BINDIR = capture([PG_CONFIG, "--bindir"], shell=False).rstrip()
os.environ["PATH"] = PG_BINDIR + os.pathsep + os.environ["PATH"]


def get_pg_major_version():
    full_version_string = run(
        "initdb --version", stdout=subprocess.PIPE, encoding="utf-8", silent=True
    ).stdout
    major_version_string = re.search("[0-9]+", full_version_string)
    assert major_version_string is not None
    return int(major_version_string.group(0))


PG_MAJOR_VERSION = get_pg_major_version()

OLDEST_SUPPORTED_CITUS_VERSION_MATRIX = {
    14: "10.2.0",
    15: "11.1.5",
    16: "12.1devel",
}

OLDEST_SUPPORTED_CITUS_VERSION = OLDEST_SUPPORTED_CITUS_VERSION_MATRIX[PG_MAJOR_VERSION]


def initialize_temp_dir(temp_dir):
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(temp_dir, 0o777)


def initialize_temp_dir_if_not_exists(temp_dir):
    if os.path.exists(temp_dir):
        return
    os.mkdir(temp_dir)
    # Give full access to TEMP_DIR so that postgres user can use it.
    os.chmod(temp_dir, 0o777)


def parallel_run(function, items, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(function, item, *args, **kwargs) for item in items]
        for future in futures:
            future.result()


def initialize_db_for_cluster(pg_path, rel_data_path, settings, node_names):
    subprocess.run(["mkdir", rel_data_path], check=True)

    def initialize(node_name):
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        command = [
            os.path.join(pg_path, "initdb"),
            "--pgdata",
            abs_data_path,
            "--username",
            USER,
            "--no-sync",
            # --allow-group-access is used to ensure we set permissions on
            # private keys correctly
            "--allow-group-access",
            "--encoding",
            "UTF8",
            "--locale",
            "POSIX",
        ]
        subprocess.run(command, check=True)
        add_settings(abs_data_path, settings)

    parallel_run(initialize, node_names)


def add_settings(abs_data_path, settings):
    conf_path = os.path.join(abs_data_path, "postgresql.conf")
    with open(conf_path, "a") as conf_file:
        for setting_key, setting_val in settings.items():
            setting = "{setting_key} = '{setting_val}'\n".format(
                setting_key=setting_key, setting_val=setting_val
            )
            conf_file.write(setting)


def create_role(pg_path, node_ports, user_name):
    def create(port):
        command = (
            "SET citus.enable_ddl_propagation TO OFF;"
            + "SELECT worker_create_or_alter_role('{}', 'CREATE ROLE {} WITH LOGIN CREATEROLE CREATEDB;', NULL)".format(
                user_name, user_name
            )
        )
        utils.psql(pg_path, port, command)
        command = "SET citus.enable_ddl_propagation TO OFF; GRANT CREATE ON DATABASE postgres to {}".format(
            user_name
        )
        utils.psql(pg_path, port, command)

    parallel_run(create, node_ports)


def coordinator_should_haveshards(pg_path, port):
    command = "SELECT citus_set_node_property('localhost', {}, 'shouldhaveshards', true)".format(
        port
    )
    utils.psql(pg_path, port, command)


def start_databases(
    pg_path, rel_data_path, node_name_to_ports, logfile_prefix, env_variables
):
    def start(node_name):
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        node_port = node_name_to_ports[node_name]
        command = [
            os.path.join(pg_path, "pg_ctl"),
            "start",
            "--pgdata",
            abs_data_path,
            "-U",
            USER,
            "-o",
            "-p {}".format(node_port),
            "--log",
            os.path.join(abs_data_path, logfile_name(logfile_prefix, node_name)),
        ]

        # set the application name if requires
        if env_variables != {}:
            os.environ.update(env_variables)

        subprocess.run(command, check=True)

    parallel_run(start, node_name_to_ports.keys())

    # We don't want parallel shutdown here because that will fail when it's
    # tried in this atexit call with an error like:
    # cannot schedule new futures after interpreter shutdown
    atexit.register(
        stop_databases,
        pg_path,
        rel_data_path,
        node_name_to_ports,
        logfile_prefix,
        no_output=True,
        parallel=False,
    )


def create_citus_extension(pg_path, node_ports):
    def create(port):
        utils.psql(pg_path, port, "CREATE EXTENSION citus;")

    parallel_run(create, node_ports)


def run_pg_regress(pg_path, pg_srcdir, port, schedule):
    should_exit = True
    try:
        _run_pg_regress(pg_path, pg_srcdir, port, schedule, should_exit)
    finally:
        subprocess.run("bin/copy_modified", check=True)


def run_pg_regress_without_exit(
    pg_path,
    pg_srcdir,
    port,
    schedule,
    output_dir=".",
    input_dir=".",
    user="postgres",
    extra_tests="",
):
    should_exit = False
    exit_code = _run_pg_regress(
        pg_path,
        pg_srcdir,
        port,
        schedule,
        should_exit,
        output_dir,
        input_dir,
        user,
        extra_tests,
    )
    copy_binary_path = os.path.join(input_dir, "copy_modified_wrapper")

    exit_code |= subprocess.call(copy_binary_path)
    return exit_code


def _run_pg_regress(
    pg_path,
    pg_srcdir,
    port,
    schedule,
    should_exit,
    output_dir=".",
    input_dir=".",
    user="postgres",
    extra_tests="",
):
    command = [
        os.path.join(pg_srcdir, "src/test/regress/pg_regress"),
        "--port",
        str(port),
        "--schedule",
        schedule,
        "--bindir",
        pg_path,
        "--user",
        user,
        "--dbname",
        "postgres",
        "--inputdir",
        input_dir,
        "--outputdir",
        output_dir,
        "--use-existing",
    ]
    if extra_tests != "":
        command.append(extra_tests)

    exit_code = subprocess.call(command)
    if should_exit and exit_code != 0:
        sys.exit(exit_code)
    return exit_code


def save_regression_diff(name, output_dir):
    path = os.path.join(output_dir, "regression.diffs")
    if not os.path.exists(path):
        return
    new_file_path = os.path.join(output_dir, "./regression_{}.diffs".format(name))
    print("new file path:", new_file_path)
    shutil.move(path, new_file_path)


def stop_metadata_to_workers(pg_path, worker_ports, coordinator_port):
    for port in worker_ports:
        command = (
            "SELECT * from stop_metadata_sync_to_node('localhost', {port});".format(
                port=port
            )
        )
        utils.psql(pg_path, coordinator_port, command)


def add_coordinator_to_metadata(pg_path, coordinator_port):
    command = "SELECT citus_set_coordinator_host('localhost');"
    utils.psql(pg_path, coordinator_port, command)


def add_workers(pg_path, worker_ports, coordinator_port):
    for port in worker_ports:
        command = "SELECT * from master_add_node('localhost', {port});".format(
            port=port
        )
        utils.psql(pg_path, coordinator_port, command)


def logfile_name(logfile_prefix, node_name):
    return "logfile_" + logfile_prefix + "_" + node_name


def stop_databases(
    pg_path,
    rel_data_path,
    node_name_to_ports,
    logfile_prefix,
    no_output=False,
    parallel=True,
):
    def stop(node_name):
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        node_port = node_name_to_ports[node_name]
        command = [
            os.path.join(pg_path, "pg_ctl"),
            "stop",
            "--pgdata",
            abs_data_path,
            "-U",
            USER,
            "-o",
            "-p {}".format(node_port),
            "--log",
            os.path.join(abs_data_path, logfile_name(logfile_prefix, node_name)),
        ]
        if no_output:
            subprocess.call(
                command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        else:
            subprocess.call(command)

    if parallel:
        parallel_run(stop, node_name_to_ports.keys())
    else:
        for node_name in node_name_to_ports.keys():
            stop(node_name)


def is_citus_set_coordinator_host_udf_exist(pg_path, port):
    return utils.psql_capture(pg_path, port, IS_CITUS_VERSION_11_SQL) == b" t\n\n"


def initialize_citus_cluster(bindir, datadir, settings, config):
    # In case there was a leftover from previous runs, stop the databases
    stop_databases(
        bindir, datadir, config.node_name_to_ports, config.name, no_output=True
    )
    initialize_db_for_cluster(
        bindir, datadir, settings, config.node_name_to_ports.keys()
    )
    start_databases(
        bindir, datadir, config.node_name_to_ports, config.name, config.env_variables
    )
    create_citus_extension(bindir, config.node_name_to_ports.values())

    # In upgrade tests, it is possible that Citus version < 11.0
    # where the citus_set_coordinator_host UDF does not exist.
    if is_citus_set_coordinator_host_udf_exist(bindir, config.coordinator_port()):
        add_coordinator_to_metadata(bindir, config.coordinator_port())

    add_workers(bindir, config.worker_ports, config.coordinator_port())
    if not config.is_mx:
        stop_metadata_to_workers(bindir, config.worker_ports, config.coordinator_port())

    config.setup_steps()


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


# this is out of ephemeral port range for many systems hence
# it is a lower chance that it will conflict with "in-use" ports
PORT_LOWER_BOUND = 10200

# ephemeral port start on many Linux systems
PORT_UPPER_BOUND = 32768

next_port = PORT_LOWER_BOUND


def cleanup_test_leftovers(nodes):
    """
    Cleaning up test leftovers needs to be done in a specific order, because
    some of these leftovers depend on others having been removed. They might
    even depend on leftovers on other nodes being removed. So this takes a list
    of nodes, so that we can clean up all test leftovers globally in the
    correct order.
    """
    for node in nodes:
        node.cleanup_subscriptions()

    for node in nodes:
        node.cleanup_publications()

    for node in nodes:
        node.cleanup_logical_replication_slots()

    for node in nodes:
        node.cleanup_schemas()

    for node in nodes:
        node.cleanup_users()


class PortLock:
    """PortLock allows you to take a lock an a specific port.

    While a port is locked by one process, other processes using PortLock won't
    get the same port.
    """

    def __init__(self):
        global next_port
        first_port = next_port
        while True:
            next_port += 1
            if next_port >= PORT_UPPER_BOUND:
                next_port = PORT_LOWER_BOUND

            # avoid infinite loop
            if first_port == next_port:
                raise Exception("Could not find port")

            self.lock = filelock.FileLock(Path(gettempdir()) / f"port-{next_port}.lock")
            try:
                self.lock.acquire(timeout=0)
            except filelock.Timeout:
                continue

            if FORCE_PORTS:
                self.port = next_port
                break

            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                try:
                    s.bind(("127.0.0.1", next_port))
                    self.port = next_port
                    break
                except Exception:
                    self.lock.release()
                    continue

    def release(self):
        """Call release when you are done with the port.

        This way other processes can use it again.
        """
        self.lock.release()


class QueryRunner(ABC):
    """A subclassable interface class that can be used to run queries.

    This is mostly useful to be generic across differnt types of things that
    implement the Postgres interface, such as Postgres, PgBouncer, or a Citus
    cluster.

    This implements some helpers send queries in a simpler manner than psycopg
    allows by default.
    """

    @abstractmethod
    def set_default_connection_options(self, options: dict[str, typing.Any]):
        """Sets the default connection options on the given options dictionary

        This is the only method that the class that subclasses QueryRunner
        needs to implement.
        """
        ...

    def make_conninfo(self, **kwargs) -> str:
        self.set_default_connection_options(kwargs)
        return psycopg.conninfo.make_conninfo(**kwargs)

    def conn(self, *, autocommit=True, **kwargs):
        """Open a psycopg connection to this server"""
        self.set_default_connection_options(kwargs)
        return psycopg.connect(
            autocommit=autocommit,
            **kwargs,
        )

    def aconn(self, *, autocommit=True, **kwargs):
        """Open an asynchronous psycopg connection to this server"""
        self.set_default_connection_options(kwargs)
        return psycopg.AsyncConnection.connect(
            autocommit=autocommit,
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

    def sql_value(self, query, params=None, allow_empty_result=False, **kwargs):
        """Run an SQL query that returns a single cell and return this value

        This opens a new connection and closes it once the query is done
        """
        with self.cur(**kwargs) as cur:
            cur.execute(query, params=params)
            result = cur.fetchall()

            if allow_empty_result and len(result) == 0:
                return None

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

        conninfo = self.make_conninfo(**kwargs)

        run(
            ["psql", "-X", f"{conninfo}", "-c", query],
            shell=False,
            silent=True,
        )

    def poll_query_until(self, query, params=None, expected=True, **kwargs):
        """Run query repeatedly until it returns the expected result"""
        start = datetime.now()
        result = None

        while start + TIMEOUT_DEFAULT > datetime.now():
            result = self.sql_value(
                query, params=params, allow_empty_result=True, **kwargs
            )
            if result == expected:
                return

            time.sleep(0.1)

        raise Exception(
            f"Timeout reached while polling query, last result was: {result}"
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

    def debug(self):
        print("Connect manually to:\n   ", repr(self.make_conninfo()))
        print("Press Enter to continue running the test...")
        input()

    def psql_debug(self, **kwargs):
        conninfo = self.make_conninfo(**kwargs)
        run(
            ["psql", f"{conninfo}"],
            shell=False,
            silent=True,
        )


class Postgres(QueryRunner):
    """A class that represents a Postgres instance on this machine

    You can query it by using the interface provided by QueryRunner or use many
    of the helper methods.
    """

    def __init__(self, pgdata):
        self.port_lock = PortLock()

        # These values should almost never be changed after initialization
        self.host = "127.0.0.1"
        self.port = self.port_lock.port

        # These values can be changed when needed
        self.dbname = "postgres"
        self.user = "postgres"
        self.schema = None

        self.pgdata = pgdata
        self.log_path = self.pgdata / "pg.log"

        # Used to track objects that we want to clean up at the end of a test
        self.subscriptions = set()
        self.publications = set()
        self.logical_replication_slots = set()
        self.schemas = set()
        self.users = set()

    def set_default_connection_options(self, options):
        options.setdefault("host", self.host)
        options.setdefault("port", self.port)
        options.setdefault("dbname", self.dbname)
        options.setdefault("user", self.user)
        if self.schema is not None:
            options.setdefault("options", f"-c search_path={self.schema}")
        options.setdefault("connect_timeout", 3)
        # needed for Ubuntu 18.04
        options.setdefault("client_encoding", "UTF8")

    def initdb(self):
        run(
            f"initdb -A trust --nosync --username postgres --pgdata {self.pgdata} --allow-group-access --encoding UTF8 --locale POSIX",
            stdout=subprocess.DEVNULL,
        )

        with self.conf_path.open(mode="a") as pgconf:
            # Allow connecting over unix sockets
            pgconf.write("unix_socket_directories = '/tmp'\n")

            # Useful logs for debugging issues
            pgconf.write("log_replication_commands = on\n")
            # The following to are also useful for debugging, but quite noisy.
            # So better to enable them manually by uncommenting.
            # pgconf.write("log_connections = on\n")
            # pgconf.write("log_disconnections = on\n")

            # Enable citus
            pgconf.write("shared_preload_libraries = 'citus'\n")

            # Allow CREATE SUBSCRIPTION to work
            pgconf.write("wal_level = 'logical'\n")
            # Faster logical replication status update so tests with logical replication
            # run faster
            pgconf.write("wal_receiver_status_interval = 1\n")

            # Faster logical replication apply worker launch so tests with logical
            # replication run faster. This is used in ApplyLauncherMain in
            # src/backend/replication/logical/launcher.c.
            pgconf.write("wal_retrieve_retry_interval = '250ms'\n")

            # Make sure there's enough logical replication resources for most
            # of our tests
            pgconf.write("max_logical_replication_workers = 50\n")
            pgconf.write("max_wal_senders = 50\n")
            pgconf.write("max_worker_processes = 50\n")
            pgconf.write("max_replication_slots = 50\n")

            # We need to make the log go to stderr so that the tests can
            # check what is being logged.  This should be the default, but
            # some packagings change the default configuration.
            pgconf.write("log_destination = stderr\n")
            # We don't need the logs anywhere else than stderr
            pgconf.write("logging_collector = off\n")

            # This makes tests run faster and we don't care about crash safety
            # of our test data.
            pgconf.write("fsync = false\n")

            # conservative settings to ensure we can run multiple postmasters:
            pgconf.write("shared_buffers = 1MB\n")
            # limit disk space consumption, too:
            pgconf.write("max_wal_size = 128MB\n")

            # don't restart after crashes to make it obvious that a crash
            # happened
            pgconf.write("restart_after_crash = off\n")

        os.truncate(self.hba_path, 0)
        self.ssl_access("all", "trust")
        self.nossl_access("all", "trust")
        self.commit_hba()

    def init_with_citus(self):
        self.initdb()
        self.start()
        self.sql("CREATE EXTENSION citus")

        # Manually turn on ssl, so that we can safely truncate
        # postgresql.auto.conf later. We can only do this after creating the
        # citus extension because that creates the self signed certificates.
        with self.conf_path.open(mode="a") as pgconf:
            pgconf.write("ssl = on\n")

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
            print(f"\n\nPG_LOG: {self.pgdata}\n")
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
        # Sadly UNIX signals are asynchronous, so we sleep a bit and hope that
        # Postgres actually processed the SIGHUP signal after the sleep.
        time.sleep(0.1)

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

    def prepare_reset(self):
        """Prepares all changes to reset Postgres settings and objects

        To actually apply the prepared changes a restart might still be needed.
        """
        self.reset_hba()
        os.truncate(self.pgdata / "postgresql.auto.conf", 0)

    def reset(self):
        """Resets any changes to Postgres settings from previous tests"""
        self.prepare_reset()
        self.restart()

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

    def configure(self, *configs):
        """Configure specific Postgres settings using ALTER SYSTEM SET

        NOTE: after configuring a call to reload or restart is needed for the
        settings to become effective.
        """
        for config in configs:
            self.sql(f"alter system set {config}")

    def log_handle(self):
        """Returns the opened logfile at the current end of the log

        By later calling read on this file you can read the contents that were
        written from this moment on.

        IMPORTANT: This handle should be closed once it's not needed anymore
        """
        f = self.log_path.open()
        f.seek(0, os.SEEK_END)
        return f

    @contextmanager
    def log_contains(self, re_string, times=None):
        """Checks if during this with block the log matches re_string

        re_string:
            The regex to search for.
        times:
            If None, any number of matches is accepted. If a number, only that
            specific number of matches is accepted.
        """
        with self.log_handle() as f:
            yield
            content = f.read()
            if times is None:
                assert re.search(re_string, content)
            else:
                match_count = len(re.findall(re_string, content))
                assert match_count == times

    def create_user(self, name, args: typing.Optional[psycopg.sql.Composable] = None):
        self.users.add(name)
        if args is None:
            args = sql.SQL("")
        self.sql(sql.SQL("CREATE USER {} {}").format(sql.Identifier(name), args))

    def create_schema(self, name):
        self.schemas.add(name)
        self.sql(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    def create_publication(self, name: str, args: psycopg.sql.Composable):
        self.publications.add(name)
        self.sql(sql.SQL("CREATE PUBLICATION {} {}").format(sql.Identifier(name), args))

    def create_logical_replication_slot(
        self, name, plugin, temporary=False, twophase=False
    ):
        self.logical_replication_slots.add(name)
        self.sql(
            "SELECT pg_catalog.pg_create_logical_replication_slot(%s,%s,%s,%s)",
            (name, plugin, temporary, twophase),
        )

    def create_subscription(self, name: str, args: psycopg.sql.Composable):
        self.subscriptions.add(name)
        self.sql(
            sql.SQL("CREATE SUBSCRIPTION {} {}").format(sql.Identifier(name), args)
        )

    def cleanup_users(self):
        for user in self.users:
            self.sql(sql.SQL("DROP USER IF EXISTS {}").format(sql.Identifier(user)))

    def cleanup_schemas(self):
        for schema in self.schemas:
            self.sql(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )

    def cleanup_publications(self):
        for publication in self.publications:
            self.sql(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                    sql.Identifier(publication)
                )
            )

    def cleanup_logical_replication_slots(self):
        for slot in self.logical_replication_slots:
            self.sql(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                (slot,),
            )

    def cleanup_subscriptions(self):
        for subscription in self.subscriptions:
            try:
                self.sql(
                    sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(
                        sql.Identifier(subscription)
                    )
                )
            except psycopg.errors.UndefinedObject:
                # Subscription didn't exist already
                continue
            self.sql(
                sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name = NONE)").format(
                    sql.Identifier(subscription)
                )
            )
            self.sql(
                sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(subscription))
            )

    def lsn(self, mode):
        """Returns the lsn for the given mode"""
        queries = {
            "insert": "SELECT pg_current_wal_insert_lsn()",
            "flush": "SELECT pg_current_wal_flush_lsn()",
            "write": "SELECT pg_current_wal_lsn()",
            "receive": "SELECT pg_last_wal_receive_lsn()",
            "replay": "SELECT pg_last_wal_replay_lsn()",
        }
        return self.sql_value(queries[mode])

    def wait_for_catchup(self, subscription_name, mode="replay", target_lsn=None):
        """Waits until the subscription has caught up"""
        if target_lsn is None:
            target_lsn = self.lsn("write")

        # Before release 12 walreceiver just set the application name to
        # "walreceiver"
        self.poll_query_until(
            sql.SQL(
                """
            SELECT {} <= {} AND state = 'streaming'
            FROM pg_catalog.pg_stat_replication
            WHERE application_name IN ({}, 'walreceiver')
            """
            ).format(target_lsn, sql.Identifier(f"{mode}_lsn"), subscription_name)
        )

    @contextmanager
    def _enable_firewall(self):
        """Enables the firewall for the platform that you are running

        Normally this should not be called directly, and instead drop_traffic
        or reject_traffic should be used.
        """
        fw_token = None
        if BSD:
            if MACOS:
                command_stderr = sudo(
                    "pfctl -E", stderr=subprocess.PIPE, text=True
                ).stderr
                match = re.search(r"^Token : (\d+)", command_stderr, flags=re.MULTILINE)
                assert match is not None
                fw_token = match.group(1)
            sudo(
                'bash -c "'
                f"echo 'anchor \\\"port_{self.port}\\\"'"
                f' | pfctl -a citus_test -f -"'
            )
        try:
            yield
        finally:
            if MACOS:
                sudo(f"pfctl -X {fw_token}")

    @contextmanager
    def drop_traffic(self):
        """Drops all TCP packets to this query runner"""
        with self._enable_firewall():
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
                    f"| pfctl -a citus_test/port_{self.port} -f -'"
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
                    sudo(f"pfctl -a citus_test/port_{self.port} -F all")

    @contextmanager
    def reject_traffic(self):
        """Rejects all traffic to this query runner with a TCP RST message"""
        with self._enable_firewall():
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
                    f"| pfctl -a citus_test/port_{self.port} -f -'"
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
                    sudo(f"pfctl -a citus_test/port_{self.port} -F all")


class CitusCluster(QueryRunner):
    """A class that represents a Citus cluster on this machine

    The nodes in the cluster can be accessed directly using the coordinator,
    workers, and nodes properties.

    If it doesn't matter which of the nodes in the cluster is used to run a
    query, then you can use the methods provided by QueryRunner directly on the
    cluster. In that case a random node will be chosen to run your query.
    """

    def __init__(self, basedir: Path, worker_count: int):
        self.coordinator = Postgres(basedir / "coordinator")
        self.workers = [Postgres(basedir / f"worker{i}") for i in range(worker_count)]
        self.nodes = [self.coordinator] + self.workers
        self._schema = None
        self.failed_reset = False

        parallel_run(Postgres.init_with_citus, self.nodes)
        with self.coordinator.cur() as cur:
            cur.execute(
                "SELECT pg_catalog.citus_set_coordinator_host(%s, %s)",
                (self.coordinator.host, self.coordinator.port),
            )
            for worker in self.workers:
                cur.execute(
                    "SELECT pg_catalog.citus_add_node(%s, %s)",
                    (worker.host, worker.port),
                )

    def set_default_connection_options(self, options):
        random.choice(self.nodes).set_default_connection_options(options)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value
        for node in self.nodes:
            node.schema = value

    def reset(self):
        """Resets any changes to Postgres settings from previous tests"""
        parallel_run(Postgres.prepare_reset, self.nodes)
        parallel_run(Postgres.restart, self.nodes)

    def cleanup(self):
        parallel_run(Postgres.cleanup, self.nodes)

    def debug(self):
        """Print information to stdout to help with debugging your cluster"""
        print("The nodes in this cluster and their connection strings are:")
        for node in self.nodes:
            print(f"{node.pgdata}:\n   ", repr(node.make_conninfo()))
        print("Press Enter to continue running the test...")
        input()
