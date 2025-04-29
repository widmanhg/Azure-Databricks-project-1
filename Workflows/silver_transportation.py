# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Silver_transportation = Job.from_dict(
    {
        "name": "Silver_transportation",
        "tasks": [
            {
                "task_key": "Lookup_Locations",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/gerardohernandezwidman@wodman.onmicrosoft.com/Netflix_project/3_LookupNotebook",
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": "0428-213015-3biqkc74",
            },
            {
                "task_key": "SilverNotebook",
                "depends_on": [
                    {
                        "task_key": "Lookup_Locations",
                    },
                ],
                "for_each_task": {
                    "inputs": "{{tasks.Lookup_Locations.values.my_arr}}",
                    "task": {
                        "task_key": "SilverNotebook_iteration",
                        "notebook_task": {
                            "notebook_path": "/Workspace/Users/gerardohernandezwidman@wodman.onmicrosoft.com/Netflix_project/2_silver",
                            "base_parameters": {
                                "sourcefolder": "{{input.sourcefolder}}",
                                "targetfolder": "{{input.targetfolder}}",
                            },
                            "source": "WORKSPACE",
                        },
                        "existing_cluster_id": "0428-213015-3biqkc74",
                    },
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Silver_transportation, job_id=798517584214941)
# or create a new job using: w.jobs.create(**Silver_transportation.as_shallow_dict())
