import sys
import json
import os
import random
import re
import logging

# Configure logging format before creating
# any logger instance.
logging.basicConfig(
    format="[%(asctime)s][%(levelname)s][%(filename)s][%(lineno)s]: %(message)s",
    level=logging.INFO
)

import datetime
import concurrent.futures
from typing import List
from utils.file import *
from utils.das import *
from utils.stats_rest import Stats2
from schemas.dataset import *

# Stats2 client
cookie_path: str = os.getenv("STATS_COOKIE_PATH")
stats: Stats2 = Stats2(cookie=cookie_path)

# Logger
logger = logging.getLogger()

# Constant
OUTPUT_FOLDER = f"{os.getcwd()}/output"


def __retrieve_dataset_info(dataset: ChildDataset) -> Optional[ChildDataset]:
    """
    Retrieve the dataset information for a given dataset name

    Args:
        dataset (ChildDataset): Dataset to complete its data

    Returns:
        ChildDataset: Given dataset object with other attributes included
        None: If the dataset information in DAS indicates it is INVALID or the name
            is not adecuate.
    """
    if not dataset.metadata.valid:
        logger.warning(
            "Dataset name doesn't comply the filter: %s",
            dataset.metadata
        )
        return None

    # Retrieve the information from DAS
    dataset_info = None
    try:
        dataset_info = das_get_dataset_info(dataset=dataset.metadata.full_name)
    except ValueError as e:
        logger.warning(e)
        return None

    # Complete the data object attributes.
    summary, info = dataset_info
    events: int = summary.get("nevents", -2)
    runs: List[int] = das_get_runs(dataset=dataset.metadata.full_name)
    dataset_type: str = info.get("status", "ERROR")

    dataset.events = events
    dataset.runs = runs
    dataset.type = dataset_type
    dataset.campaign = page_metadata.campaign(dataset.metadata)

    # Retrieve the PrepID and workflow data from Stats2
    stats2_info: Optional[List[dict]] = stats.get_output_dataset(
        output_dataset=dataset.metadata.full_name
    )
    if stats2_info:
        raw_data = stats2_info[0]
        stats_data: Stats2Information = Stats2Information(
            dataset=dataset.metadata.full_name,
            raw=raw_data
        )
        dataset.prepid = stats_data.prepid
        dataset.workflow = stats_data.workflow

    return dataset


def build_relationship(
    dataset: ChildDataset, remaining_data_tiers: List[str]
) -> ChildDataset:
    """
    Performs an in-depth search, recursively looking for the
    children datasets for a given one.

    Args:
        dataset (ChildDataset): Dataset to retrieve its children.
        remaining_data_tiers (list[str]): Data tiers remaining
            to scan.
    Returns:
        ChildDataset: Requested dataset with all its children.
    """
    # Base case: There are not remaining data tiers to check
    # or there are no children for the current dataset.
    base_case_reached: bool = False
    if not remaining_data_tiers:
        base_case_reached = True

    # Explore in-depth
    children_datasets: List[ChildDataset] = []
    if not base_case_reached:
        inmediate_next: str = remaining_data_tiers[0]
        inmediate_children: List[ChildDataset] = das_scan_children(
            dataset=dataset.metadata, next_tier=inmediate_next
        )
        all_children: List[ChildDataset] = list(set(inmediate_children + dataset.output))
        
        # Recursive case: Search in-depth for the children
        for cd in all_children:
            children: Optional[ChildDataset] = build_relationship(
                dataset=cd,
                remaining_data_tiers=remaining_data_tiers[1:]
            )
            if children:
                children_datasets.append(children)

    # For the base case, retrieve the dataset information
    # and parse the hierarchy
    dataset: Optional[ChildDataset] = __retrieve_dataset_info(dataset=dataset)
    if dataset:
        dataset.output = children_datasets

    return dataset


def match_era_datasets(
    raw_metadata: DatasetMetadata,
) -> List[ChildDataset]:
    """
    Queries the children datasets linked to a RAW datasets and filters
    them to match only those specified in the desired era

    Args:
        raw_dataset (str): RAW data set metadata

    Returns:
        List[ChildDataset]: A list with all the children datasets for the
            given RAW dataset grouped by data tier.
    """
    DESIRED_DATA_TIERS: List[str] = ["AOD", "MINIAOD", "NANOAOD"]
    raw_as_child: ChildDataset = ChildDataset(metadata=raw_metadata)
    childrens: Optional[ChildDataset] = build_relationship(
        dataset=raw_as_child,
        remaining_data_tiers=DESIRED_DATA_TIERS
    )
    return childrens.output if childrens else []


def get_dataset_info(dataset: str) -> Optional[RAWDataset]:
    """
    For a RAW dataset, this function retrieves its metadata
    and all the sublevel datasets filtered by the interest campaigns and
    processing strings.

    Args:
        dataset (str): Name of the dataset to retrieve
        year_info (dict): Eras for the desired year

    Returns:
        RAWDataset: RAW dataset information
        None: If RAW dataset status is not 'VALID' or 'PRODUCTION'
    """
    raw_metadata: DatasetMetadata = DatasetMetadata(name=dataset)
    dataset_content: Optional[Tuple[dict, dict]] = das_get_dataset_info(dataset=dataset)
    if not dataset_content:
        logger.error(
            "The requested dataset, %s, does not exist or its status is not valid or production",
            dataset,
        )
        return None

    dataset_summary, _ = dataset_content
    events: int = dataset_summary.get("nevents", -1)
    runs: List[int] = das_get_runs(dataset=dataset)

    # Parse the dataset's year from the era attributes
    raw_dataset: RAWDataset = RAWDataset(
        dataset=dataset, 
        events=events, 
        year=raw_metadata.year, 
        runs=runs
    )

    # Retrieve the sublevel datasets
    logger.info("Querying for the sublevel datasets for RAW dataset: %s" % dataset)
    interest_children_datasets: List[ChildDataset] = match_era_datasets(raw_metadata=raw_metadata)
    if interest_children_datasets:
        raw_dataset.output = interest_children_datasets
    else:
        logger.error("No child datasets for RAW dataset: %s", dataset)

    return raw_dataset


# Load dataset names using file
start_time = datetime.datetime.now()
datasets = load_datasets_from_file(path="data/datasets.txt")
logger.info("Read %s datasets from file", len(datasets))

if "--debug" in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:10]
    logger.debug("Picking random %s datasets because debug", len(datasets))

datasets = sorted(datasets)
with open("data/years.json", "r", encoding="utf-8") as file:
    years = json.load(file)

# Metadata information
page_metadata: PageMetadata = PageMetadata(metadata=years)

# Concurrent executor
MAX_EXECUTORS = 25
BREAKER = len(datasets)
results: List[RAWDataset] = []
dataset_args: List[Tuple[str, dict]] = []

# Retrieve the dataset name and its year
for index, raw_dataset in enumerate(datasets):
    if index == BREAKER:
        break
    dataset_args.append((raw_dataset,))

# Use a concurrent execution to retrieve the data
logger.info("Scannning %d datasets", len(dataset_args))
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_EXECUTORS) as executor:
    future_result = {
        executor.submit(get_dataset_info, *dataset_args): dataset_args[0]
        for dataset_args in dataset_args
    }
    for future in concurrent.futures.as_completed(future_result):
        raw_dataset = future_result[future]
        logger.info("Data retrieved for RAW dataset: %s", raw_dataset)
        try:
            data = future.result()
            results.append(data)
            logger.info("Datasets processed: %d/%d", len(results), len(dataset_args))
        except Exception as exc:
            logger.error(
                "Error processing RAW dataset: %s",
                raw_dataset,
                exc_info=True,
            )

missing = len(dataset_args) - len(results)
logger.warning("Missing data for %s datasets", missing)
logger.info("Storing data")

# Order the datasets, their children and parse as a dict.
results_dict: List[dict] = [
    rd.dict
    for rd in sorted(results, key=lambda raw_d: raw_d.dataset)
]

with open(f"{OUTPUT_FOLDER}/data.json", "w") as output_file:
    json.dump(results_dict, output_file, indent=1, sort_keys=True)

end_time = datetime.datetime.now()
elapsed = end_time - start_time
rate = round(elapsed.total_seconds() / len(results), 2)
logger.info("Elapsed time: %s", end_time - start_time)
logger.info("Rate: %s s/dataset", rate)