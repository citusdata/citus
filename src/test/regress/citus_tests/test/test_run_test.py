from run_test import TestDeps as Dependencies
from run_test import merge_test_dependencies, needed_worker_count
from run_test import test_dependencies as get_test_dependencies


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
        ),
    ]

    merged_dependencies = merge_test_dependencies(dependencies)

    assert merged_dependencies.schedule == "base_schedule"
    assert merged_dependencies.direct_extra_tests == ["first_setup", "second_setup"]
    assert not merged_dependencies.repeatable
    assert needed_worker_count("unconfigured_test", merged_dependencies) == 5
