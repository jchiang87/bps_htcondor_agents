from datetime import datetime, timedelta
import multiprocessing
from lsst.ctrl.bps.htcondor import condor_history, condor_q, update_job_info
from lsst.ctrl.bps.htcondor.lssthtc import _locate_schedds as locate_schedds


__all__ = ("condor_search",)


SCHEDDS = locate_schedds(locate_all=True)


def _condor_search(constraint=None, hist=None, schedds=None, **kwds):
    """Version of ctrl.bps.htcondor.condor_search that allows one
    to pass **kwds.
    """
    if schedds is None:
        schedds = SCHEDDS
    job_info = condor_q(constraint=constraint, schedds=schedds, **kwds)
    if hist is not None:
        epoch = (datetime.now() - timedelta(days=hist)).timestamp()
        constraint += (f" && (CompletionDate >= {epoch} "
                       f"|| JobFinishedHookDone >= {epoch})")
        hist_info = condor_history(constraint, schedds=schedds, **kwds)
        update_job_info(job_info, hist_info)
    return job_info


class MultiprocCondorSearch:
    """Wrapper for condor_search that uses multiprocessing to run
    in parallel over schedds
    """
    def __init__(self):
        pass

    @staticmethod
    def _run_condor_search(node, *args, **kwds):
        """Search using for a single schedd."""
        kwds['schedds'] = {node: SCHEDDS[node]}
        return _condor_search(*args, **kwds)

    def __call__(self, constraint=None, hist=None, processes=None,
                 projection=None, schedds=None):
        if schedds is None:
            schedds = SCHEDDS
        if processes is None:
            processes = len(schedds)

        if processes == 1:
            kwds = {"constraint": constraint,
                    "hist": hist,
                    "schedds": schedds}
            if projection is not None:
                kwds["projection"] = projection
            return _condor_search(**kwds)

        run_info = {}
        with multiprocessing.Pool(processes=processes) as pool:
            workers = []
            for node in schedds:
                args = (node,)
                kwds = {"constraint": constraint,
                        "hist": hist}
                if projection is not None:
                    kwds["projection"] = projection
                workers.append(pool.apply_async(self._run_condor_search,
                                                args, kwds))
            pool.close()
            pool.join()
            for worker in workers:
                run_info.update(worker.get())
        return run_info


condor_search = MultiprocCondorSearch()
