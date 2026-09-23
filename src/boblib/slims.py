from datetime import UTC, datetime, timedelta
from functools import cache
from typing import cast

from thicks.criteria import equals, greater_than_or_equal, is_one_of
from thicks.slims import Record, Slims

from boblib.record import ProtocolRunRecord, ResultRecord, StatusRecord


class NoMatch(Exception): ...


class MultiMatch(Exception): ...


@cache
def get_slims_connection(
    url: str,
    username: str,
    password: str,
) -> Slims:
    return Slims(
        "Bob",
        url=url,
        username=username,
        password=password,
    )


@cache
def get_content_by_barcode(
    slims: Slims,
    barcode: str,
) -> Record:
    samples = slims.fetch("Content", equals("cntn_barCode", barcode))
    if not samples:
        raise NoMatch(f"No content found with barcode {barcode}")
    if len(samples) > 1:
        raise MultiMatch(f"Multiple contents found with barcode {barcode}")
    return samples[0]


@cache
def get_queues_for_workflow(
    slims: Slims,
    workflow_pk: int,
) -> list[int]:
    """Get the primary keys of all queues associated with a given workflow.

    Queues are used to identify templates associated with a given workflow, as the protocol templates themselves
    do not directly reference workflows in SLIMS. Instead, templates reference input/output queues, which in turn
    reference workflow queues, which reference workflows.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        workflow_pk (int): The primary key of the workflow for which to retrieve queues.

        if not libraries:
            raise NoMatch(f"No sample found with barcode {self.slims_sample_barcode}")
        if len(libraries) > 1:
            raise MultiMatch(f"Multiple samples found with barcode {self.slims_sample_barcode}")
        return libraries[0]
        list[int]: A list of primary keys of queues associated with the specified workflow.

    Raises:
        NoMatch: If no queues are found for the specified workflow.
    """
    queues = {
        wfqu.wfqu_fk_queue.value
        for wfqu in slims.fetch(
            "WorkflowQueue",
            equals("wfqu_fk_workflow", workflow_pk),
        )
    }
    if not queues:
        raise NoMatch(f"No queues found for workflow {workflow_pk}")
    return [*queues]


@cache
def get_protocol_steps_for_test(
    slims: Slims,
    test_pk: int,
) -> list[int]:
    """Get the primary keys of all protocol steps associated with a given test.

    A protocol step is the definition of a single block inside a protocol template. A protocol step may reference
    a protocol step test, which in turn links the block to a specific test.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        test_pk (int): The primary key of the test for which to retrieve protocol steps.

    Returns:
        list[int]: A list of primary keys of protocol steps associated with the specified test.

    Raises:
        NoMatch: If no protocol steps are found for the specified test.
    """
    step_pks = {
        xsts.xsts_fk_experimentStep.value
        for xsts in slims.fetch(
            "ExperimentStepTest",
            equals("xsts_fk_test", test_pk)  # nofmt
            & equals("xsts_active", True),
        )
    }
    if not step_pks:
        raise NoMatch(f"No Protocol Steps match test with pk={test_pk}")
    return [*step_pks]


@cache
def get_protocol_templates_for_test(
    slims: Slims,
    test_pk: int,
) -> list[int]:
    """Get the primary keys of all protocol templates associated with a given test.

    A protocol template is a definition of a protocol, which reference one or more blocks (protocol steps).
    To find the protocol templates associated with a given test, we first find the protocol steps associated
    with the test, and then find the protocol templates associated with those steps.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        test_pk (int): The primary key of the test for which to retrieve protocol templates.

    Returns:
        list[int]: A list of primary keys of protocol templates associated with the specified test.

    Raises:
        NoMatch: If no protocol templates are found for the specified test.
    """
    test_steps = get_protocol_steps_for_test(slims, test_pk)
    template_pks = {
        xpst.xpst_fk_experimentTemplate.value
        for xpst in slims.fetch(
            "ExperimentStep",
            is_one_of("xpst_pk", test_steps)  # nofmt
            & equals("xpst_active", True),
        )
    }
    if not template_pks:
        raise NoMatch(f"No Protocol Templates match test with pk={test_pk}")
    return [*template_pks]


@cache
def get_template_for_test_in_workflow(
    slims: Slims,
    test_pk: int,
    workflow_pk: int,
) -> int:
    """Get the primary key of the protocol template associated with a given test in a specific workflow.

    Note that we expect there to be only one protocol referencing a given test in a given workflow.
    If there is more than one protocol referencing a given test in a given workflow, this is considered an error.
    This is not a limitation in SLIMS, but we need to enforce this as Bob expects exactly one protocol with exactly
    one block where it can place the results of a given test (i.e. pre-processing or pipeline metadata.)

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        test_pk (int): The primary key of the test for which to retrieve the protocol template.
        workflow_pk (int): The primary key of the workflow in which to search for the protocol template.

    Returns:
        int: The primary key of the protocol template associated with the specified test in the specified workflow.

    Raises:
        NoMatch: If no protocol template is found for the specified test in the specified workflow.
        MultiMatch: If multiple protocol templates are found for the specified test in the specified workflow.

    """
    test_templates = get_protocol_templates_for_test(slims, test_pk)
    workflow_queues = get_queues_for_workflow(slims, workflow_pk)
    latest_templates = slims.fetch(
        "ExperimentTemplate",
        is_one_of("xptm_pk", test_templates)
        & is_one_of("xptm_fk_from_queue", workflow_queues)
        & is_one_of("xptm_fk_to_queue", workflow_queues)
        & equals("xptm_versionType", "VERSION")
        & equals("xptm_active", True),
    )
    if len(latest_templates) == 0:
        raise NoMatch(
            f"No template found for test with pk={test_pk} "  # nofmt
            f"in workflow with pk={workflow_pk}"
        )
    if len(latest_templates) > 1:
        raise MultiMatch(
            f"Multiple templates found for test with pk={test_pk} "  # nofmt
            f"and workflow with pk={workflow_pk}"
        )

    return latest_templates[0].pk()


@cache
def get_test_run_step(
    slims: Slims,
    run_pk: int,
    test_pk: int,
) -> int:
    """Retrieve the protocol run step associated with a specific test in a specific run.

    A protocol run step represents the execution of a specific protocol step (block) within a protocol run.
    In order to create a result for a specific test in a protocol run, we need to find the protocol run step
    that corresponds to the test in the specified run.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        run_pk (int): The primary key of the protocol run.
        test_pk (int): The primary key of the test for which to retrieve the run step.

    Returns:
        int: The primary key of the protocol run step associated with the specified test in the specified run.

    Raises:
        NoMatch: If no protocol run step is found for the specified test in the specified run.
        MultiMatch: If multiple protocol run steps are found for the specified test in the specified run.
    """
    test_steps = get_protocol_steps_for_test(slims, test_pk)
    test_run_steps = slims.fetch(
        "ExperimentRunStep",
        equals("xprs_fk_experimentRun", run_pk)  # nofmt
        & is_one_of("xprs_fk_experimentStep", test_steps),
    )

    if not test_run_steps:
        raise NoMatch(
            f"No Protocol Run Steps match test with pk={test_pk} "  # nofmt
            f"in run with pk={run_pk}"
        )
    if len(test_run_steps) > 1:
        raise MultiMatch(
            f"Multiple Protocol Run Steps match test with pk={test_pk} "  # nofmt
            f"in run with pk={run_pk}"
        )
    return test_run_steps[0].pk()


@cache
def get_input_run_step(
    slims: Slims,
    run_pk: int,
) -> int:
    """Retrieve the input protocol run step for a specific protocol run.

    The input protocol run step is the first step in a protocol run, which is used to link content (e.g., samples)
    to the protocol run. There is no documentation on how to identify the input protocol run step, but based on
    observations, it appears that the input protocol run step has `xpst_type` equal to `"POP_STEP"`.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        run_pk (int): The primary key of the protocol run for which to retrieve the input run step.
    """
    input_run_steps = slims.fetch(
        "ExperimentRunStep",
        equals("xprs_fk_experimentRun", run_pk)  # nofmt
        & equals("xpst_type", "POP_STEP"),
        # FIXME: POP_STEP is undocumented; is there a better way?
    )
    if not input_run_steps:
        raise NoMatch(f"No input Protocol Run Step found for run with pk={run_pk}")
    if len(input_run_steps) > 1:
        raise MultiMatch(f"Multiple input Protocol Run Steps found for run with pk={run_pk}")
    return input_run_steps[0].pk()


@cache
def get_test_by_pk(
    slims: Slims,
    test_pk: int,
) -> Record:
    """Retrieve a test record by its primary key.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        test_pk (int): The primary key of the test to retrieve.

    Returns:
        Record: The test record matching the specified primary key.

    Raises:
        NoMatch: If no test is found with the specified primary key.
        MultiMatch: If multiple tests are found with the specified primary key.
    """
    test = slims.fetch_by_pk("Test", test_pk)
    if not test:
        raise NoMatch(f"No Test found with pk '{test_pk}'")
    return test


@cache
def get_test_by_name(
    slims: Slims,
    test_name: str,
) -> Record:
    """Retrieve a test record by its name.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        test_name (str): The name of the test to retrieve.

    Returns:
        Record: The test record matching the specified name.

    Raises:
        NoMatch: If no test is found with the specified name.
        MultiMatch: If multiple tests are found with the specified name.
    """
    tests = slims.fetch("Test", equals("test_name", test_name), start=0, end=2)
    if not tests:
        raise NoMatch(f"No Test found with name '{test_name}'")
    if len(tests) > 1:
        raise MultiMatch(f"Multiple Tests found with name '{test_name}'")
    return tests[0]


@cache
def get_results_by_protocol_run(slims: Slims, run_pk: int | tuple[int], test_name: str | None = None) -> list[Record]:
    """Retrieve the results associated with a specific protocol run.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        run_pk (int | list[int]): The primary key(s) of the protocol run(s).
        test_name (str | None): Optionally, the name of the test to filter results by.

    Returns:
        list[Record]: The result record(s) associated with the specified protocol run(s).

    Raises:
        NoMatch: If no test is found for the specified protocol run.
        MultiMatch: If multiple tests are found for the specified protocol run.
    """
    if isinstance(run_pk, int):
        run_pk = (run_pk,)
    run_steps = slims.fetch("ExperimentRunStep", is_one_of("xprs_fk_experimentRun", [*run_pk]))
    if not run_steps:
        raise NoMatch(f"No Experiment Run Steps found for test protocol run(s) with pk(s) '{run_pk}'")
    criteria = is_one_of("rslt_fk_experimentRunStep", [s.pk() for s in run_steps])
    if test_name is not None:
        criteria &= equals("test_name", test_name)
    results = slims.fetch("Result", criteria)
    if not results:
        raise NoMatch(f"No Results found for test '{test_name}' in protocol run(s) with pk(s) '{run_pk}'")
    return results

@cache
def get_workflow_by_uid(
    slims: Slims,
    workflow_uid: str,
) -> Record:
    """Retrieve a workflow record by its UID.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        workflow_uid (str): The UID of the workflow to retrieve.

    Returns:
        Record: The workflow record matching the specified UID.

    Raises:
        NoMatch: If no workflow is found with the specified UID.
        MultiMatch: If multiple workflows are found with the specified UID.
    """
    workflows = slims.fetch("Workflow", equals("wrfl_uniqueIdentifier", workflow_uid), start=0, end=2)
    if not workflows:
        raise NoMatch(f"No Workflow found with UID '{workflow_uid}'")
    if len(workflows) > 1:
        raise MultiMatch(f"Multiple Workflows found with UID '{workflow_uid}'")
    return workflows[0]


@cache
def get_status_by_table_and_id(
    slims: Slims,
    table: str,
    status_id: str,
) -> StatusRecord:
    """Retrieve a status record by its ID and table.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        status_id (str): The ID of the status to retrieve.
        table (str): The table in which to look for the status.

    Returns:
        StatusRecord: The status record matching the specified ID and table.

    Raises:
        NoMatch: If no status is found with the specified ID and table.
        MultiMatch: If multiple statuses are found with the specified ID and table.
    """
    statuses = slims.fetch(table, equals("stts_uniqueIdentifier", status_id) & equals("dbtb_name", table), start=0, end=2)
    if not statuses:
        raise NoMatch(f"No Status found with ID '{status_id}' in table '{table}'")
    if len(statuses) > 1:
        raise MultiMatch(f"Multiple Statuses found with ID '{status_id}' in table '{table}'")
    return cast(StatusRecord, statuses[0])

@cache
def get_status_by_pk(
    slims: Slims,
    status_pk: int,
) -> StatusRecord:
    """Retrieve a status record by its primary key.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        status_pk (int): The primary key of the status to retrieve.

    Returns:
        StatusRecord: The status record matching the specified primary key.

    Raises:
        NoMatch: If no status is found with the specified primary key.
    """
    status = slims.fetch_by_pk("Status", status_pk)
    if not status:
        raise NoMatch(f"No Status found with PK '{status_pk}'")
    return cast(StatusRecord, status)

def link_content_to_run_step(slims: Slims, content_pk: int, run_step_pk: int):
    """Link a content record to a protocol run step.

    The python SLIMS API does not provide a method to link content to a protocol run step, so we have to use the raw API.
    """
    slims.slims_api.post(f"eln/content/input/{run_step_pk}/{content_pk}").raise_for_status()


def unlink_content_from_run_step(slims: Slims, content_pk: int, run_step_pk: int):
    """Unlink a content record from a protocol run step.

    The python SLIMS API does not provide a method to unlink content from a protocol run step, so we have to use the raw API.
    """
    slims.slims_api.delete(f"eln/content/input/{run_step_pk}/{content_pk}").raise_for_status()


def create_protocol_run_for_test_in_workflow(
    slims: Slims,
    test_pk: int,
    workflow_pk: int,
    name: str,
) -> Record:
    """Create a protocol run for a test in a workflow.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        workflow_pk (int): The primary key of the workflow in which to create the protocol run.
        test_pk (int): The primary key of the test for which to create the protocol run.
        name (str): The name of the protocol run to be created.

    Returns:
        Record: The newly created protocol run record.
    """
    template_pk = get_template_for_test_in_workflow(slims, test_pk, workflow_pk)
    record = slims.add(
        "ExperimentRun",
        {
            "xprn_usage": "ELN",
            "xprn_name": name,
            "xprn_fk_workflow": workflow_pk,
            "xprn_fk_experimentTemplate": template_pk,
        },
    )
    return cast(ProtocolRunRecord, record)


def link_content_to_protocol_run(
    slims: Slims,
    content_pk: int,
    run_pk: int,
):
    """Link a content record to a protocol run.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        content_pk (int): The primary key of the content record to be linked.
        run_pk (int): The primary key of the protocol run to which the content record will be linked.
    """
    input_run_step_pk = get_input_run_step(slims, run_pk)
    link_content_to_run_step(slims, content_pk, input_run_step_pk)


def create_test_result_for_content_in_protocol_run(
    slims: Slims,
    run_pk: int,
    test_pk: int,
    content_pk: int,
    **kwargs,
) -> ResultRecord:
    """Create a result for a test in a protocol run, associated with a specific content record.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        run_pk (int): The primary key of the protocol run in which to create the result.
        test_pk (int): The primary key of the test for which to create the result.
        content_pk (int): The primary key of the content record to be associated with the result.

    Returns:
        Record: The newly created result record.
    """
    run_step_pk = get_test_run_step(slims, run_pk=run_pk, test_pk=test_pk)
    record = slims.add(
        "Result",
        {
            "rslt_fk_content": content_pk,
            "rslt_fk_test": test_pk,
            "rslt_fk_experimentRunStep": run_step_pk,
            **kwargs,
        },
    )
    return cast(ResultRecord, record)


def get_protocol_runs_for_test_in_workflow(
    slims: Slims,
    workflow_pk: int,
    test_pk: int,
    max_age: timedelta,
    cancelled: bool = False,
) -> list[ProtocolRunRecord]:
    """List all protocol runs for a specific test in a workflow.

    Args:
        slims (Slims): An instance of the Slims class to interact with the SLIMS API.
        workflow_pk (int): The primary key of the workflow.
        test_pk (int): The primary key of the test.

    Returns:
        list[ProtocolRunRecord]: A list of protocol run records for the specified test in the workflow.
    """
    test_templates = get_protocol_templates_for_test(slims, test_pk)
    max_date = int((datetime.now(UTC) - max_age).timestamp() * 1e3)
    criteria = (
        equals("xprn_fk_workflow", workflow_pk)
        & is_one_of("xprn_fk_experimentTemplate", test_templates)
        & greater_than_or_equal("xprn_createdOn", max_date)
    )
    if not cancelled:
        criteria &= ~equals("xprn_cancelled", True)
    records = slims.fetch(
        "ExperimentRun",
        sort=["xprn_createdOn"],
        criteria=criteria,
    )
    return cast(list[ProtocolRunRecord], records)
