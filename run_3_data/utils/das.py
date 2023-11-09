"""
This module includes some wrappers to make some queries to
DAS CLI client
"""
import json
import os
import logging
from typing import List, Tuple, Optional
from schemas.dataset import DatasetMetadata


logger: logging.Logger = logging.getLogger()

# dasgoclient package location
DASGOCLIENT_PACKAGE: str = "/cvmfs/cms.cern.ch/common/dasgoclient"


def das_get_datasets_names(query: str) -> List[str]:
    """
    Given a query, retrieve from DAS the name of all the datatsets that fulfill the condition.

    Args:
        query (str): Query to perform to DAS.

    Returns:
        list[str]: The name of all datasets that fulfill the condition
    """
    stream: str = os.popen("%s --query='%s'" % (DASGOCLIENT_PACKAGE, query)).read()
    result: List[str] = [r.strip() for r in stream.split("\n") if r.strip()]
    return result


def das_get_runs(dataset) -> List[int]:
    """
    Get the list of runs for the desired dataset.
    """
    if not dataset:
        return []

    result = set()
    try:
        stream = os.popen(
            "%s --query='run dataset=%s'" % (DASGOCLIENT_PACKAGE, dataset)
        )
        result = set([int(r.strip()) for r in stream.read().split("\n") if r.strip()])
        logger.info("Got %s runs for %s" % (len(result), dataset))
    except Exception as ex:
        logger.error("Error getting %s runs :%s" % (dataset, str(ex)))

    return list(result)


def das_get_events(dataset):
    """
    Get the list of events for the desired dataset.
    """
    if not dataset:
        return 0
    result = 0
    try:
        result = int(
            os.popen(
                "%s --query='dataset=%s' | grep dataset.nevents"
                % (DASGOCLIENT_PACKAGE, dataset)
            )
            .read()
            .strip()
        )
    except Exception as ex:
        logger.error("Error getting events for %s: %s" % (dataset, str(ex)))
    return result


def das_get_dataset_info(dataset: str) -> Optional[Tuple[dict, dict]]:
    """
    Retrieves the dataset info and its summary from DAS

    Args:
        dataset (str): Name of the dataset to query

    Returns:
        Tuple[dict, dict]: If the dataset has files and its status is valid or production.
            The first dict is the dbs3:filesummaries object
            and the second dict is the dbs3:dataset_info.
        None: If the dataset does not have files or if its status is not valid or production.
    """
    FILE_SUMMARY = "dbs3:filesummaries"
    DATASET_INFO = "dbs3:dataset_info"

    command = "%s --query='%s' --json" % (DASGOCLIENT_PACKAGE, dataset)
    stream = os.popen(command).read()
    json_result = json.loads(stream)

    file_summary = {}
    dataset_info = {}
    for obj in json_result:
        if obj["das"]["services"][0] == FILE_SUMMARY:
            file_summary = obj["dataset"][0]
        elif obj["das"]["services"][0] == DATASET_INFO:
            dataset_info = obj["dataset"][0]

    if (
        dataset_info.get("status") in ("PRODUCTION", "VALID")
        and file_summary.get("nfiles") > 0
    ):
        return file_summary, dataset_info

    return None


def das_scan_children(dataset: DatasetMetadata, next_tier: str) -> List[DatasetMetadata]:
    """
    For a given dataset name and the children data tier
    query all the possible children data sets and retrieve its names
    using the child relationship and the processing string.

    Args:
        dataset (DatasetMetadata): Current dataset to query for its children
            datasets.
        next_tier (str): Child data tier to look for.

    Returns:
        list[DatasetMetadata]: List of all the children datasets for the given one.
    """
    children: List[str] = []

    # Create a wildcard query for the next data tier
    next_tier_query: str = f"{dataset.full_name.rsplit('/', 1)[0]}/{next_tier}"
    wildcard_query: str = f"dataset status=* dataset='{next_tier_query}'"
    children += das_get_datasets_names(query=wildcard_query)

    # Also retrieve other possible children with different PS
    child_query: str = f"child dataset='{dataset.full_name}'"
    child_datasets: List[str] = das_get_datasets_names(query=child_query)
    children += child_datasets

    # Remove duplicates and just pick the ones related to the next data tier
    children_metadata: List[DatasetMetadata] = [
        DatasetMetadata(name=ds)
        for ds in list(set(children))
    ]
    children_metadata: List[DatasetMetadata] = [
        cd
        for cd in children_metadata
        if cd.datatier == next_tier
    ]

    return children_metadata
