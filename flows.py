from prefect import Flow, task
from prefect.agent.local import LocalAgent
from prefect.executors import LocalExecutor
from prefect.schedules import CronSchedule

from .am_diffs.compute_am_diffs import compute_and_dispatch_diff
from .backup_bo_database import backup_bo_database
from .data_build.load_ams_in_ovh import load_ams_in_ovh


@task
def compute_and_dispatch_diff_task() -> None:
    compute_and_dispatch_diff()


def compute_and_dispatch_diff_flow() -> Flow:
    flow = Flow('compute-and-dispatch-diff', tasks=[compute_and_dispatch_diff_task], executor=LocalExecutor())
    flow.schedule = CronSchedule('0 2 * * 0')  # Every sunday at 2 am
    return flow


@task
def load_ams_in_ovh_task() -> None:
    load_ams_in_ovh()


def load_ams_in_ovh_flow() -> Flow:
    flow = Flow('load-am-in-ovh', tasks=[load_ams_in_ovh_task], executor=LocalExecutor())
    flow.schedule = CronSchedule('0 1 * * 0')  # Every sunday at 1 am
    return flow


@task
def backup_bo_database_task() -> None:
    backup_bo_database()


def backup_bo_database_flow() -> Flow:
    flow = Flow('backup-bo-database', tasks=[backup_bo_database_task], executor=LocalExecutor())
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
