from collections import defaultdict, namedtuple
import pandas as pd
from lsst.ctrl.bps.htcondor.lssthtc import read_node_status
from .condor_search import condor_search


__all__ = ("find_workflows", "extract_jobs_status")


WorkflowParams = namedtuple("WorkflowParams",
                            ["run_id", "node", "bps_run", "submit_dir"])


def find_workflows(user, hist):
    constraint = f'Owner=="{user}" && JobUniverse==7'
    projection = ["bps_run", "Iwd", "ClusterId", "JobBatchId"]
    results = {}
    for node, run_info in condor_search(
            constraint=constraint,
            hist=hist,
            projection=projection,
    ).items():
        for run_id, info in run_info.items():
            if "bps_run" not in info:
                continue
            my_run_id = int(float(run_id))
            results[my_run_id] = WorkflowParams(
                my_run_id, node, info["bps_run"], info["Iwd"]
            )
    return results


def extract_jobs_status(submit_dir):
    jobs = read_node_status(submit_dir)
    data = defaultdict(list)
    for job_id, info in jobs.items():
        data['job_id'].append(job_id)
        data['node'].append(info['Node'])
        data['node_status'].append(info['NodeStatus'])
        if 'ToE' in info:
            data['ExitCode'].append(info['ToE']['ExitCode'])
            data['ExitBySignal'].append(info['ToE']['ExitBySignal'])
        else:
            data['ExitCode'].append(-1)
            data['ExitBySignal'].append(None)
        data['wms_node_type'].append(info['wms_node_type'].name)
        if 'SlotName' in info:
            worker_node = info['SlotName'].split('@')[1].split('.')[0]
        else:
            worker_node = None
        data['worker_node'].append(worker_node)
        data['bps_job_label'].append(info['bps_job_label'])
    return pd.DataFrame(data), jobs
