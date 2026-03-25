import os
import yaml
from collections import defaultdict
from datetime import datetime
import pandas as pd
from pandas import DataFrame
from opensearchpy import OpenSearch
from smolagents import tool


__all__ = (
    "get_index_info",
    "index_properties",
    "find_workflows",
    "find_failed_jobs",
    "find_failed_runs",
    "extract_job_status",
)


with open("/sdf/home/j/jchiang/.lsst/secrets") as fobj:
    auth = list(yaml.safe_load(fobj).items())[0]

host = "usdf-opensearch.slac.stanford.edu"
port = 443
OSCLIENT = OpenSearch(hosts=[{'host': host, "port": port}],
                      http_compress=True, http_auth=auth,
                      use_ssl=True, verify_certs=True,
                      ssl_assert_hostname=False, ssl_show_warn=False)


def get_index_info(index="htcondor*", client=OSCLIENT):
    indices = client.cat.indices(index=index, format="json")
    data = defaultdict(list)
    for item in indices:
        for k, v in item.items():
            data[k].append(v)
    return pd.DataFrame(data)


def index_properties(index="htcondor-history-v1", client=OSCLIENT):
    mapping = client.indices.get_mapping(index=index)
    properties = mapping[index]["mappings"]["properties"]
    return properties


def find_workflows(user="jchiang", columns=None, size=10000,
                   index="htcondor-history-v1", client=OSCLIENT):
    if columns is None:
        columns = ['JobBatchId', 'JobCurrentStartDate', 'bps_run',
                   'Iwd', 'JobStartDate']
    body = {
        'size': size,
        'query': {
            'bool': {
                'filter': [
                    {'terms': {'Owner': [user]}},
                    {'terms': {'JobUniverse': [7]}}
                ]
            }
        },
        '_source': columns
    }
    response = client.search(body=body, index=index)
    data = defaultdict(list)
    for item in response['hits']['hits']:
        row = item['_source']
        if "bps_run" not in row:
            continue
        for column in columns:
            if column not in row:
                value = None
            else:
                value = row[column]
            if column == 'JobCurrentStartDate' and value is not None:
                value = datetime.fromtimestamp(value)
            data[column].append(value)
    df0 = pd.DataFrame(data)
    if 'JobCurrentStartDate' in df0:
        df0 = df0.sort_values('JobCurrentStartDate', ignore_index=True)
    return df0


@tool
def find_failed_runs(user: str,
                     hist: int = 30,
                     run_substr: str | None = None) -> DataFrame:
    """Find all runs with failed jobs for workflows launched by a
    specific user.  This function will search over a look-back time
    history and subject to substring matching in the bps_run name.

    Args:
        user: The username of the user who submitted the workflow.  This is
            the "Owner" constraint in the OpenSearch query.
        hist: The number of days prior to now over which the OpenSearch
            query will be made.
        run_substr: The substring constraint of the "bps_run" column.
        size: The number of documents to return for each OpenSearch scroll
            iteration.

    Returns:
        A pandas data frame with columns including "JobBatchId",
        "JobStartDate", "bps_run", "submit_dir", and "num_failed_jobs".
        Each workflow instance is uniquely identified by either "JobBatchId"
        or "bps_run".
    """
    df0 = find_failed_jobs(user, hist=hist, run_substr=run_substr)
    data = defaultdict(list)
    for job_batch_id in sorted(set(df0['JobBatchId'])):
        df = df0.query(f"JobBatchId=={job_batch_id}")
        data['JobBatchId'].append(job_batch_id)
        data['bps_run'].append(df.iloc[0]['bps_run'])
        data['JobStartDate'].append(min(df['JobStartDate']))
        iwd = df.iloc[0]['Iwd']
        submit_dir = iwd[:iwd.find("/jobs")]
        data['submit_dir'].append(submit_dir)
        data['num_failed_jobs'].append(len(df))
    return pd.DataFrame(data)


@tool
def find_failed_jobs(user: str,
                     hist: int = 30,
                     run_substr: str | None = None,
                     size: int = 10000) -> DataFrame:
    """Find info for failed jobs in workflows launched by a specific user.
    This function will search over a look-back time history and
    subject to substring matching in the bps_run name.

    Args:
        user: The username of the user who submitted the workflow.  This is
            the "Owner" constraint in the OpenSearch query.
        hist: The number of days prior to now over which the OpenSearch
            query will be made.
        run_substr: The substring constraint of the "bps_run" column.
        size: The number of documents to return for each OpenSearch scroll
            iteration.

    Returns:
        A pandas data frame with columns including "JobBatchId",
        "JobStartDate", "log_file", "bps_run" for each failed job.
        Each workflow instance is uniquely identified by either "JobBatchId"
        or "bps_run".

    """

    JobStartDate_min = int(datetime.timestamp(datetime.now()) - hist*86400.0)  # noqa: N806, E501
    index = "htcondor-history-v1"
    client = OSCLIENT
    columns = [
        "JobBatchId",
        "JobStartDate",
        "Err",
        "Iwd",
        "bps_job_name",
        "bps_run",
        "bps_job_label"
    ]
    filter_items = [
        {"terms": {"Owner": [user]}},
        {"range": {"ExitCode": {"gt": 0}}},
        {"range": {"JobStartDate": {"gt": JobStartDate_min}}},
    ]
    if run_substr is not None:
        filter_items.append(
            {"wildcard": {"bps_run": {"value": f"*{run_substr}*"}}}
        )
    body = {
        'query': {
            'bool': {
                "filter": filter_items
            }
        }
    }
    response = client.search(
        size=size,
        body=body,
        index=index,
        scroll="1m",
    )
    data = defaultdict(list)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    while len(hits) > 0:
        for item in hits:
            row = item['_source']
            for column in columns:
                if column not in row:
                    value = None
                else:
                    value = row[column]
                data[column].append(value)
            log_file = os.path.join(row['Iwd'], row['Err'])
            data['log_file'].append(log_file)
        response = client.scroll(
            scroll_id=scroll_id,
            scroll="1m",
        )
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
    df0 = pd.DataFrame(data)
    return df0


def extract_job_status(JobBatchId, columns=None, size=10000,  # noqa: N803
                       index="htcondor-history-v1", client=OSCLIENT):
    if columns is None:
        columns = [
            "bps_job_name",
            "ExitCode",
            "ExitBySignal",
            "ExitStatus",
            "Err",
            "Iwd",
            "bps_job_label",
            "bps_operator",
            "bps_payload",
            "bps_run",
            "user_log",
            "JobStatus",
            "StartdName",
        ]
    body = {
        'query': {
            'bool': {
                'filter': [
                    {'terms': {'JobBatchId': [JobBatchId]}},
                ]
            }
        },
        '_source': columns
    }
    response = client.search(
        size=size,
        body=body,
        index=index,
        scroll="1m",
    )
    data = defaultdict(list)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    while len(hits) > 0:
        for item in hits:
            row = item['_source']
            for column in columns:
                if column not in row:
                    value = None
                else:
                    value = row[column]
                data[column].append(value)
        response = client.scroll(
            scroll_id=scroll_id,
            scroll="1m",
        )
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
    df0 = pd.DataFrame(data)
    return df0
