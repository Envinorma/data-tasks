from prefect import Flow, task
from prefect.agent.local import LocalAgent
from prefect.executors import LocalExecutor
from prefect.schedules import CronSchedule

from am_diffs.compute_am_diffs import compute_and_dispatch_diff


@task
def compute_and_dispatch_diff_task() -> None:
    compute_and_dispatch_diff()


def compute_and_dispatch_diff_flow() -> Flow:
    flow = Flow('compute-and-dispatch-diff', tasks=[compute_and_dispatch_diff_task], executor=LocalExecutor())
    flow.schedule = CronSchedule('0 2 * * 0')  # Every sunday at 2 am
    return flow


if __name__ == '__main__':
    all_flows = [  # List of flows to run for preparing Envinorma data
        compute_and_dispatch_diff_flow(),
    ]
    for flow in all_flows:
        flow.register(project_name='Envinorma')

    # Agent to orchestrate runs and communicating with the server, can be run in a distinct process.
    agent = LocalAgent(show_flow_logs=True)
    agent.start()
