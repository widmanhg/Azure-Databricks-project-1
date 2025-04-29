# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Silver_cleaning = Job.from_dict(
    {
        "name": "Silver_cleaning",
        "tasks": [
            {
                "task_key": "WeekdayLookup",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/gerardohernandezwidman@wodman.onmicrosoft.com/Netflix_project/5_LookupNotebook",
                    "base_parameters": {
                        "weekday": "{{job.start_time.iso_weekday}}",
                    },
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": "0428-213015-3biqkc74",
            },
            {
                "task_key": "IfWeekDay",
                "depends_on": [
                    {
                        "task_key": "WeekdayLookup",
                    },
                ],
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{tasks.WeekdayLookup.values.weekoutput}}",
                    "right": "7",
                },
            },
            {
                "task_key": "Falsenotebook",
                "depends_on": [
                    {
                        "task_key": "IfWeekDay",
                        "outcome": "false",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/gerardohernandezwidman@wodman.onmicrosoft.com/Netflix_project/6_false",
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": "0428-213015-3biqkc74",
            },
            {
                "task_key": "Silver_Master_Data",
                "depends_on": [
                    {
                        "task_key": "IfWeekDay",
                        "outcome": "true",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/gerardohernandezwidman@wodman.onmicrosoft.com/Netflix_project/4_silver",
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": "0428-213015-3biqkc74",
            },
        ],
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Silver_cleaning, job_id=298285597616364)
# or create a new job using: w.jobs.create(**Silver_cleaning.as_shallow_dict())
