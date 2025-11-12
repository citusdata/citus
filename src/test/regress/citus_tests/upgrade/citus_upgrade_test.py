#!/usr/bin/env python3

"""citus_upgrade_test
Usage:
    citus_upgrade_test [options] --bindir=<bindir> --pgxsdir=<pgxsdir> [--citus-old-version=<citus-old-version>]

Options:
    --bindir=<bindir>                       The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --citus-pre-tar=<citus-pre-tar>         Tarball with the citus artifacts to use as the base version to upgrade from
    --citus-post-tar=<citus-post-tar>       Tarball with the citus artifacts to use as the new version to upgrade to
    --pgxsdir=<pgxsdir>           	        Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --citus-old-version=<citus-old-version> Citus old version for local run(ex v8.0.0)
    --mixed                                 Run the verification phase with one node not upgraded.
    --minor-upgrade                         Use minor version upgrade test schedules instead of major version schedules.
"""

import multiprocessing
import os
import re
import subprocess
import sys

from docopt import docopt

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ignore E402 because these imports require addition to path
import common  # noqa: E402
import utils  # noqa: E402
from common import CI, PG_MAJOR_VERSION, REPO_ROOT, run  # noqa: E402
from utils import USER  # noqa: E402

from config import (  # noqa: E402
    AFTER_CITUS_UPGRADE_COORD_SCHEDULE,
    BEFORE_CITUS_UPGRADE_COORD_SCHEDULE,
    CITUS_VERSION_SQL,
    MASTER_VERSION,
    MIXED_AFTER_CITUS_UPGRADE_SCHEDULE,
    MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE,
    CitusUpgradeConfig,
)


def main(config):
    before_upgrade_schedule = get_before_upgrade_schedule(config.mixed_mode)
    after_upgrade_schedule = get_after_upgrade_schedule(config.mixed_mode)
    run_citus_upgrade_tests(config, before_upgrade_schedule, after_upgrade_schedule)


def run_citus_upgrade_tests(config, before_upgrade_schedule, after_upgrade_schedule):
    install_citus(config.pre_tar_path)
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )

    report_initial_version(config)

    # Store the pre-upgrade GUCs and UDFs for minor version upgrades
    pre_upgrade = None
    if config.minor_upgrade:
        pre_upgrade = get_citus_catalog_info(config)

    run_test_on_coordinator(config, before_upgrade_schedule)

    remove_citus(config.pre_tar_path)
    if after_upgrade_schedule is None:
        return

    install_citus(config.post_tar_path)

    restart_databases(config.bindir, config.datadir, config.mixed_mode, config)
    run_alter_citus(config.bindir, config.mixed_mode, config)
    verify_upgrade(config, config.mixed_mode, config.node_name_to_ports.values())

    # For minor version upgrades, verify GUCs and UDFs does not have breaking changes
    breaking_changes = []
    if config.minor_upgrade:
        breaking_changes = compare_citus_catalog_info(config, pre_upgrade)

    run_test_on_coordinator(config, after_upgrade_schedule)
    remove_citus(config.post_tar_path)

    # Fail the test if there are any breaking changes
    if breaking_changes:
        common.eprint("\n=== BREAKING CHANGES DETECTED ===")
        for change in breaking_changes:
            common.eprint(f"  - {change}")
        common.eprint("==================================\n")
        sys.exit(1)


def get_citus_catalog_info(config):

    results = {}
    # Store GUCs
    guc_results = utils.psql_capture(
        config.bindir,
        config.coordinator_port(),
        "SELECT name, boot_val FROM pg_settings WHERE name LIKE 'citus.%' ORDER BY name;",
    )

    guc_lines = guc_results.decode("utf-8").strip().split("\n")
    results["gucs"] = {}
    for line in guc_lines[2:]:  # Skip header lines
        name, boot_val = line.split("|")
        results["gucs"][name.strip()] = boot_val.strip()

    # Store UDFs
    udf_results = utils.psql_capture(
        config.bindir,
        config.coordinator_port(),
        """
        SELECT
        n.nspname AS schema_name,
        p.proname AS function_name,
        pg_get_function_arguments(p.oid) AS full_args,
        pg_get_function_result(p.oid) AS return_type
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_depend d ON d.objid = p.oid
        JOIN pg_extension e ON e.oid = d.refobjid
        WHERE e.extname = 'citus'
        AND d.deptype = 'e'
        ORDER BY schema_name, function_name, full_args;
        """,
    )

    udf_lines = udf_results.decode("utf-8").strip().split("\n")
    results["udfs"] = {}
    for line in udf_lines[2:]:  # Skip header lines
        schema_name, function_name, full_args, return_type = line.split("|")
        key = (schema_name.strip(), function_name.strip())
        signature = (full_args.strip(), return_type.strip())

        if key not in results["udfs"]:
            results["udfs"][key] = set()
        results["udfs"][key].add(signature)

    # Store types, exclude composite types (t.typrelid = 0) and
    # exclude auto-created array types
    # (t.typname LIKE '\_%' AND t.typelem <> 0)
    type_results = utils.psql_capture(
        config.bindir,
        config.coordinator_port(),
        """
        SELECT n.nspname, t.typname, t.typtype
        FROM pg_type t
        JOIN pg_depend d ON d.objid = t.oid
        JOIN pg_extension e ON e.oid = d.refobjid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE e.extname = 'citus'
        AND t.typrelid = 0
        AND NOT (t.typname LIKE '\\_%%' AND t.typelem <> 0)
        ORDER BY n.nspname, t.typname;
        """,
    )
    type_lines = type_results.decode("utf-8").strip().split("\n")
    results["types"] = {}

    for line in type_lines[2:]:  # Skip header lines
        nspname, typname, typtype = line.split("|")
        key = (nspname.strip(), typname.strip())
        results["types"][key] = typtype.strip()

    # Store tables and views
    table_results = utils.psql_capture(
        config.bindir,
        config.coordinator_port(),
        """
        SELECT n.nspname, c.relname, a.attname, t.typname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON t.oid = a.atttypid
        JOIN pg_depend d ON d.objid = c.oid
        JOIN pg_extension e ON e.oid = d.refobjid
        WHERE e.extname = 'citus'
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY n.nspname, c.relname, a.attname;
        """,
    )

    table_lines = table_results.decode("utf-8").strip().split("\n")
    results["tables"] = {}
    for line in table_lines[2:]:  # Skip header lines
        nspname, relname, attname, typname = line.split("|")
        key = (nspname.strip(), relname.strip())

        if key not in results["tables"]:
            results["tables"][key] = {}
        results["tables"][key][attname.strip()] = typname.strip()

    return results


def compare_citus_catalog_info(config, pre_upgrade):
    post_upgrade = get_citus_catalog_info(config)
    breaking_changes = []

    # Compare GUCs
    for name, boot_val in pre_upgrade["gucs"].items():
        if name not in post_upgrade["gucs"]:
            breaking_changes.append(f"GUC {name} was removed")
        elif post_upgrade["gucs"][name] != boot_val and name != "citus.version":
            breaking_changes.append(
                f"The default value of GUC {name} was changed from {boot_val} to {post_upgrade['gucs'][name]}"
            )

    # Compare UDFs - check if any pre-upgrade signatures were removed
    for (schema_name, function_name), pre_signatures in pre_upgrade["udfs"].items():
        if (schema_name, function_name) not in post_upgrade["udfs"]:
            breaking_changes.append(
                f"UDF {schema_name}.{function_name} was completely removed"
            )
        else:
            post_signatures = post_upgrade["udfs"][(schema_name, function_name)]
            removed_signatures = pre_signatures - post_signatures

            if removed_signatures:
                for full_args, return_type in removed_signatures:
                    if not find_compatible_udf_signature(
                        full_args, return_type, post_signatures
                    ):
                        breaking_changes.append(
                            f"UDF signature removed: {schema_name}.{function_name}({full_args}) RETURNS {return_type}"
                        )

    # Compare Types - check if any pre-upgrade types were removed or changed
    for (nspname, typname), typtype in pre_upgrade["types"].items():
        if (nspname, typname) not in post_upgrade["types"]:
            breaking_changes.append(f"Type {nspname}.{typname} was removed")
        elif post_upgrade["types"][(nspname, typname)] != typtype:
            breaking_changes.append(
                f"Type {nspname}.{typname} changed type from {typtype} to {post_upgrade['types'][(nspname, typname)]}"
            )

    # Compare tables / views - check if any pre-upgrade tables or columns were removed or changed
    for (nspname, relname), columns in pre_upgrade["tables"].items():
        if (nspname, relname) not in post_upgrade["tables"]:
            breaking_changes.append(f"Table/view {nspname}.{relname} was removed")
        else:
            post_columns = post_upgrade["tables"][(nspname, relname)]

            for col_name, col_type in columns.items():
                if col_name not in post_columns:
                    breaking_changes.append(
                        f"Column {col_name} in table/view {nspname}.{relname} was removed"
                    )
                elif post_columns[col_name] != col_type:
                    breaking_changes.append(
                        f"Column {col_name} in table/view {nspname}.{relname} changed type from {col_type} to {post_columns[col_name]}"
                    )

    return breaking_changes


def find_compatible_udf_signature(full_args, return_type, post_signatures):

    pre_args_list = [arg.strip() for arg in full_args.split(",") if arg.strip()]

    for post_full_args, post_return_type in post_signatures:
        if post_return_type == return_type:
            post_args_list = [
                arg.strip() for arg in post_full_args.split(",") if arg.strip()
            ]
            """ Here check if the function signatures are compatible, they are compatible if:
            post_args_list has all the arguments of pre_args_list in the same order, but can have
            additional arguments with default values """
            pre_index = 0
            post_index = 0
            compatible = True
            while pre_index < len(pre_args_list) and post_index < len(post_args_list):
                if pre_args_list[pre_index] == post_args_list[post_index]:
                    pre_index += 1
                else:
                    # Check if the argument in post_args_list has a default value
                    if "default" not in post_args_list[post_index].lower():
                        compatible = False
                        break
                post_index += 1
            if pre_index < len(pre_args_list):
                compatible = False
                continue

            while post_index < len(post_args_list):
                if "default" not in post_args_list[post_index].lower():
                    compatible = False
                    break
                post_index += 1

            if compatible:
                return True

    return False


def install_citus(tar_path):
    if tar_path:
        with utils.cd("/"):
            run(["tar", "xvf", tar_path], shell=False)
    else:
        with utils.cd(REPO_ROOT):
            run(f"make -j{multiprocessing.cpu_count()} -s install")


def report_initial_version(config):
    for port in config.node_name_to_ports.values():
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        print("port:{} citus version {}".format(port, actual_citus_version))


def get_version_number(version):
    return re.findall(r"\d+.\d+", version)[0]


def get_actual_citus_version(pg_path, port):
    citus_version = utils.psql_capture(pg_path, port, CITUS_VERSION_SQL)
    citus_version = citus_version.decode("utf-8")
    return get_version_number(citus_version)


def run_test_on_coordinator(config, schedule):
    common.run_pg_regress(
        config.bindir, config.pg_srcdir, config.coordinator_port(), schedule
    )


def remove_citus(tar_path):
    if tar_path:
        with utils.cd("/"):
            remove_tar_files(tar_path)


def remove_tar_files(tar_path):
    ps = subprocess.Popen(("tar", "tf", tar_path), stdout=subprocess.PIPE)
    subprocess.check_output(("xargs", "rm", "-v"), stdin=ps.stdout)
    ps.wait()


def restart_databases(pg_path, rel_data_path, mixed_mode, config):
    for node_name in config.node_name_to_ports.keys():
        if mixed_mode and config.node_name_to_ports[node_name] in (
            config.chosen_random_worker_port,
            config.coordinator_port(),
        ):
            continue
        abs_data_path = os.path.abspath(os.path.join(rel_data_path, node_name))
        restart_database(
            pg_path=pg_path,
            abs_data_path=abs_data_path,
            node_name=node_name,
            node_ports=config.node_name_to_ports,
            logfile_prefix=config.name,
        )


def restart_database(pg_path, abs_data_path, node_name, node_ports, logfile_prefix):
    command = [
        os.path.join(pg_path, "pg_ctl"),
        "restart",
        "--pgdata",
        abs_data_path,
        "-U",
        USER,
        "-o",
        "-p {}".format(node_ports[node_name]),
        "--log",
        os.path.join(abs_data_path, common.logfile_name(logfile_prefix, node_name)),
    ]
    subprocess.run(command, check=True)


def run_alter_citus(pg_path, mixed_mode, config):
    for port in config.node_name_to_ports.values():
        if mixed_mode and port in (
            config.chosen_random_worker_port,
            config.coordinator_port(),
        ):
            continue
        utils.psql(pg_path, port, "ALTER EXTENSION citus UPDATE;")


def verify_upgrade(config, mixed_mode, node_ports):
    for port in node_ports:
        actual_citus_version = get_actual_citus_version(config.bindir, port)
        expected_citus_version = MASTER_VERSION
        if expected_citus_version != actual_citus_version and not (
            mixed_mode
            and port in (config.chosen_random_worker_port, config.coordinator_port())
        ):
            print(
                "port: {} citus version {} expected {}".format(
                    port, actual_citus_version, expected_citus_version
                )
            )
            sys.exit(1)
        else:
            print("port:{} citus version {}".format(port, actual_citus_version))


def get_before_upgrade_schedule(mixed_mode):
    if mixed_mode:
        return MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE
    else:
        return BEFORE_CITUS_UPGRADE_COORD_SCHEDULE


def get_after_upgrade_schedule(mixed_mode):
    if mixed_mode:
        return MIXED_AFTER_CITUS_UPGRADE_SCHEDULE
    else:
        return AFTER_CITUS_UPGRADE_COORD_SCHEDULE


def generate_citus_tarball(citus_version):
    tmp_dir = "tmp_citus_tarballs"
    citus_old_tarpath = os.path.abspath(
        os.path.join(tmp_dir, f"install-pg{PG_MAJOR_VERSION}-citus{citus_version}.tar")
    )

    common.initialize_temp_dir_if_not_exists(tmp_dir)
    dirpath = os.path.dirname(os.path.realpath(__file__))
    local_script_path = os.path.join(dirpath, "generate_citus_tarballs.sh")
    with utils.cd(tmp_dir):
        subprocess.check_call([local_script_path, str(PG_MAJOR_VERSION), citus_version])

    return citus_old_tarpath


if __name__ == "__main__":
    args = docopt(__doc__, version="citus_upgrade_test")
    if not CI:
        citus_tarball_path = generate_citus_tarball(args["--citus-old-version"])
        config = CitusUpgradeConfig(args, citus_tarball_path, None)
    else:
        config = CitusUpgradeConfig(
            args, args["--citus-pre-tar"], args["--citus-post-tar"]
        )

    main(config)
