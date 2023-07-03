"""
This module includes some wrappers to make some queries to
DAS CLI client
"""
import json
import os
import logging
from typing import List, Tuple, Optional


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
    result: List[str] = [r.strip() for r in stream.split('\n') if r.strip()]
    return result

def das_get_runs(dataset) -> List[int]:
    if not dataset:
        return []

    result = set()
    try:
        stream = os.popen("%s --query='run dataset=%s'" % (DASGOCLIENT_PACKAGE, dataset))
        result = set([int(r.strip()) for r in stream.read().split('\n') if r.strip()])
        logger.info('Got %s runs for %s' % (len(result), dataset))
    except Exception as ex:
        logger.error('Error getting %s runs :%s' % (dataset, str(ex)))

    return list(result)


def das_get_events(dataset):
    if not dataset:
        return 0
    result = 0
    try:
        result = int(os.popen("%s --query='dataset=%s' | grep dataset.nevents" % (DASGOCLIENT_PACKAGE, dataset)).read().strip())
    except Exception as ex:
        logger.error('Error getting events for %s: %s' % (dataset, str(ex)))
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

    if dataset_info.get("status") in ("PRODUCTION", "VALID") and file_summary.get("nfiles") > 0:
        return file_summary, dataset_info

    return None


def das_retrieve_latest_dataset(datasets: List[str]) -> Tuple[int, Tuple[dict, dict]]:
    """
    For a group of datasets, this function will retrieve the dataset with the lastest
    date of modification and its index in the original list

    Args:
        datasets (list[str]): List of datasets to query

    Return:
        Tuple[int, Tuple[dict, dict]]: A tuple with the index of the latest dataset and its information
    """
    dataset_info: List[Optional[Tuple[dict, dict]]] = [das_get_dataset_info(d) for d in datasets]
    idx, value = max(enumerate(dataset_info), key=lambda el: el[1][1]["last_modification_date"])
    return idx, value


def das_retrieve_dataset_components(dataset: str) -> Tuple[str, str, str]:
    """
    Parses the dataset name to retrieve some attributes: primary dataset, 
    era, processing string and data tier

    Args:
        dataset (str): Dataset name

    Returns:
        Tuple[str, str, str]: Primary dataset, processing string and data tier
    """
    name_components = [c for c in dataset.strip().split("/") if c]
    primary_dataset = name_components[0]
    data_tier = name_components[-1]

    # Parse the processing string
    second_component = name_components[1]
    second_component_parts = [s for s in second_component.strip().split("-") if s]
    raw_processing_string = second_component_parts[-2] # -1 position is the version

    # Sometimes, I don't know why, there is a second subversion included into the dataset
    # underscored is its delimiter (_)
    processing_string_parts = [p for p in raw_processing_string.strip().split("_") if p]
    processing_string = processing_string_parts[0]

    return primary_dataset, processing_string, data_tier
