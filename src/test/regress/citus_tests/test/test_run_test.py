from run_test import TestDeps as Dependencies
from run_test import (
    merge_test_dependencies,
    needed_worker_count,
    run_schedule_with_multiregress,
)
from run_test import test_dependencies as get_test_dependencies
from run_test import tmp_schedule


def test_whole_schedule_line_merges_dependencies():
    args = {
        "use_base_schedule": False,
        "use_whole_schedule_line": False,
    }
    schedule_line = (
        "test: multi_deparse_shard_query multi_distributed_transaction_id "
        "intermediate_results limit_intermediate_size\n"
    )

    dependencies = get_test_dependencies(
        "intermediate_results", "multi_schedule", schedule_line, args
    )
    assert dependencies.schedule == "minimal_schedule"

    args["use_whole_schedule_line"] = True
    dependencies = get_test_dependencies(
        "intermediate_results", "multi_schedule", schedule_line, args
    )
    assert dependencies.schedule == "base_schedule"


def test_merged_dependencies_include_all_requirements():
    dependencies = [
        Dependencies(None),
        Dependencies("minimal_schedule", ["first_setup"], worker_count=3),
        Dependencies(
            "base_schedule",
            ["second_setup"],
            repeatable=False,
            worker_count=5,
            follower_cluster=True,
        ),
    ]

    merged_dependencies = merge_test_dependencies(dependencies)

    assert merged_dependencies.schedule == "base_schedule"
    assert merged_dependencies.direct_extra_tests == ["first_setup", "second_setup"]
    assert not merged_dependencies.repeatable
    assert needed_worker_count("unconfigured_test", merged_dependencies) == 5
    assert merged_dependencies.follower_cluster


def test_follower_schedule_selects_follower_custom_target(monkeypatch):
    args = {
        "use_base_schedule": False,
        "use_whole_schedule_line": False,
        "valgrind": False,
    }
    dependencies = get_test_dependencies(
        "multi_follower_dml",
        "multi_follower_schedule",
        "test: multi_follower_dml\n",
        args,
    )
    commands = []
    monkeypatch.setattr("run_test.run", commands.append)

    run_schedule_with_multiregress(
        "multi_follower_dml", "tmp_schedule", dependencies, args
    )

    assert dependencies.follower_cluster
    assert dependencies.schedule is None
    assert dependencies.extra_tests() == [
        "follower_single_node",
        "multi_follower_select_statements",
    ]
    assert "check-follower-custom-schedule" in commands[0]
    assert "WORKERCOUNT=2" in commands[0]
    assert "SCHEDULE='tmp_schedule'" in commands[0]


def test_multiuser_copy_uses_required_enterprise_setup():
    args = {
        "use_base_schedule": False,
        "use_whole_schedule_line": True,
    }
    dependencies = get_test_dependencies(
        "multi_multiuser_copy",
        "enterprise_schedule",
        "test: multi_multiuser_copy\n",
        args,
    )

    assert dependencies.schedule == "enterprise_minimal_schedule"
    assert dependencies.extra_tests() == ["multi_create_table", "multi_create_users"]
    assert dependencies.repeatable


def test_multiuser_copy_setup_precedes_repetitions(tmp_path, monkeypatch):
    args = {
        "use_base_schedule": False,
        "use_whole_schedule_line": True,
        "repeat": 8,
    }
    dependencies = get_test_dependencies(
        "multi_multiuser_copy",
        "enterprise_schedule",
        "test: multi_multiuser_copy\n",
        args,
    )
    monkeypatch.setattr("run_test.REGRESS_DIR", tmp_path)
    (tmp_path / "enterprise_minimal_schedule").write_text("test: enterprise_setup\n")

    with tmp_schedule(
        "multi_multiuser_copy",
        dependencies,
        "test: multi_multiuser_copy\n",
        args,
    ) as schedule:
        schedule_lines = (tmp_path / schedule).read_text().splitlines()

    assert schedule_lines == [
        "test: enterprise_setup",
        "test: multi_create_table",
        "test: multi_create_users",
        *(["test: multi_multiuser_copy"] * 8),
    ]
