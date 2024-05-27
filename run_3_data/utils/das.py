"""
This module includes some wrappers to make some queries to
DAS CLI client
"""
import json
import os
import logging
import pprint
from typing import List, Tuple, Optional, Dict
from schemas.dataset import DatasetMetadata, ChildDataset


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

    Raises:
        ValueError: If the dataset does not have files or if its status is not valid or production.
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

    dataset_status = dataset_info.get("status")
    dataset_events = file_summary.get("nevents", -2)
    dataset_files = file_summary.get("nfiles", -2)
    if (
        dataset_status in ("PRODUCTION", "VALID")
        and dataset_files > 0
    ):
        return file_summary, dataset_info

    error_msg: str = (
        f"Dataset ({dataset}) - Status: {dataset_status} - "
        f"Events: {dataset_events} - Files: {dataset_files}"
    )
    raise ValueError(error_msg)


def group_as_child_dataset(children: List[DatasetMetadata]) -> List[ChildDataset]:
    """
    Groups all children found from a parent dataset
    following the data tier hierarchy.

    Args:
        children (list[DatasetMetadata]): List of related children data sets.
    
    Returns:
        list[ChildDataset]: Children datasets grouped by data tier hierarchy.
    """
    # Key: (processing_str, version)
    groups: Dict[Tuple[str, str], List[DatasetMetadata]] = {}

    # Sort all the data sets per data tier.
    sorted_children: List[DatasetMetadata] = sorted(children, key=lambda c: c.datatier)

    # Group the datasets
    for child in sorted_children:
        group_key = (child.processing_string, child.version)
        groups[group_key] = groups.get(group_key, []) + [child]

    logger.debug("Child dataset groups: %s", pprint.pformat(groups, indent=4, compact=True))

    # Key: (processing_str, version)
    reduced_children: Dict[Tuple[str, str], ChildDataset] = {}

    # Cast and reduce
    for key, group_children in groups.items():
        parent: Optional[ChildDataset] = None
        reduced: Optional[ChildDataset] = None

        for child in group_children:
            current_child: ChildDataset = ChildDataset(metadata=child)
            if not parent:
                parent = current_child
                reduced = current_child
                continue

            if reduced:
                reduced.output.append(current_child)
                reduced = current_child

        if reduced.output != []:
            raise ValueError("The latest child appended should not have references to any children")
        
        reduced_children[key] = parent

    children: List[ChildDataset] = list(reduced_children.values())
    logger.debug("Child dataset result: %s", pprint.pformat(children, indent=4, compact=True))
    return children


def das_scan_children(dataset: DatasetMetadata, next_tier: str) -> List[ChildDataset]:
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
    logger.debug("Scanning children for: %s", dataset)
    DESIRED_DATA_TIERS: List[str] = ["AOD", "MINIAOD", "NANOAOD"]
    children: List[str] = []

    child_query: str = f"child dataset='{dataset.full_name}'"
    child_datasets: List[str] = das_get_datasets_names(query=child_query)
    children += child_datasets

    # Remove duplicates and filter invalid names and data tier
    children_metadata: List[DatasetMetadata] = [
        DatasetMetadata(name=ds)
        for ds in list(set(children))
    ]
    children_metadata = [
        cd
        for cd in children_metadata
        if cd.valid and cd.datatier in DESIRED_DATA_TIERS
    ]

    children: List[ChildDataset] = group_as_child_dataset(children=children_metadata)
    return children
