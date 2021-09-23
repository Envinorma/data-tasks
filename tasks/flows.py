def _set_environment_variables() -> None:
    # To keep above prefect import to ensure env vars are set correctly
    from .common.config import PSQL_DSN  # noqa: F401


_set_environment_variables()

from prefect.core import task  # noqa: E402
from prefect.core.flow import Flow  # noqa: E402
from prefect.agent.local.agent import LocalAgent  # noqa: E402
from prefect.executors.local import LocalExecutor  # noqa: E402
from prefect.schedules.schedules import CronSchedule  # noqa: E402

from .am_diffs.compute_am_diffs import compute_and_dispatch_diff  # noqa: E402
from .backup_bo_database import backup_bo_database  # noqa: E402
from .data_build.load_ams_in_ovh import load_ams_in_ovh  # noqa: E402


@task
def compute_and_dispatch_diff_task() -> None:
    compute_and_dispatch_diff()


def compute_and_dispatch_diff_flow() -> Flow:
    flow = Flow('compute-and-dispatch-diff', tasks=[compute_and_dispatch_diff_task], executor=LocalExecutor())  # type: ignore
    flow.schedule = CronSchedule('0 2 * * 0')  # Every sunday at 2 am
    return flow


@task
def load_ams_in_ovh_task() -> None:
    load_ams_in_ovh()


def load_ams_in_ovh_flow() -> Flow:
    flow = Flow('load-am-in-ovh', tasks=[load_ams_in_ovh_task], executor=LocalExecutor())  # type: ignore
    flow.schedule = CronSchedule('0 1 * * 0')  # Every sunday at 1 am
    return flow


@task
def backup_bo_database_task() -> None:
    backup_bo_database()


def backup_bo_database_flow() -> Flow:
    flow = Flow('backup-bo-database', tasks=[backup_bo_database_task], executor=LocalExecutor())  # type: ignore
    flow.schedule = CronSchedule('0 0 * * 0')  # Every sunday at 0 am
    return flow


if __name__ == '__main__':
    all_flows = [  # List of flows to run for preparing Envinorma data
        compute_and_dispatch_diff_flow(),
        load_ams_in_ovh_flow(),
        backup_bo_database_flow(),
    ]
    for flow in all_flows:
        flow.register(project_name='Envinorma')

    # Agent to orchestrate runs and communicating with the server, can be run in a distinct process.
    agent = LocalAgent(show_flow_logs=True)
    agent.start()
