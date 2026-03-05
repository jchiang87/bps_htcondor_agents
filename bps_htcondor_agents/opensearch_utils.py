import yaml
from collections import defaultdict
from datetime import datetime
import pandas as pd
from opensearchpy import OpenSearch


__all__ = ("get_index_info", "index_properties", "find_workflows",
           "extract_job_status")


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
