import sys
import json
import os
import random
import re
import logging
from typing import List
from utils.file import *
from utils.das import *
from schemas.dataset import *


# Logger
# Set up logging
logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()

# Regex pattern for year
era_regex = re.compile(r"Run([0-9]{4})([A-Z]{1})")
year_regex = re.compile(r"([0-9]{4})")

# Constant
OUTPUT_FOLDER = f"{os.getcwd()}/output"


def retrieve_latest_dataset(datasets: List[str], campaign: str, era: str) -> DatasetMatch:
    """
    From a list of datasets, retrieve the latest data set information
    using as criterion the last modification date

    Args:
        datasets (list[str]): List of datasets to filter
        campaign (str): Campaign to set to the latest dataset
        era (str): Era to set to the latest dataset

    Returns:
        DatasetMatch: Dataset information
    """
    # Choose the most recent dataset available for the category
    _, latest_dataset = das_retrieve_latest_dataset(datasets=datasets)
    summary, _ = latest_dataset
    name: str = summary.get("name")
    _, processing_string, data_tier = das_retrieve_dataset_components(dataset=name)
    info: DatasetMatch = DatasetMatch(
        campaign=campaign,
        processing_string=processing_string,
        dataset=name,
        data_tier=data_tier,
        era=era
    )
    return info


def __retrieve_dataset_info(dataset_requested: DatasetMatch) -> Optional[ChildDataset]:
    """
    Retrives the dataset information from DAS for a desired dataset and parses it
    into the required format to display this information into both tables.

    Args:
        dataset_requested (DatasetMatch): Dataset to query

    Returns:
        ChildDataset: Dataset information parsed into the required format
        None: If the status for the requested dataset is not ("PRODUCTION", "VALID")
    """
    dataset_info = das_get_dataset_info(dataset=dataset_requested.dataset)
    if not dataset_info:
        return None
    
    summary, info = dataset_info
    events: int = summary.get("nevents", -2)
    runs: List[int] = das_get_runs(dataset=dataset_requested.dataset)
    type: str = info.get("status", "ERROR")

    return ChildDataset(
        dataset=dataset_requested.dataset,
        events=events,
        runs=runs,
        type=type,
        campaign=dataset_requested.campaign,
        processing_string=dataset_requested.processing_string,
        datatier=dataset_requested.data_tier,
        era=dataset_requested.era
    )


def __build_relationship(
        era: str,
        era_data: dict,
        processing_string: str,
        group: dict
    ) -> Optional[ChildDataset]:
    """
    From a dictionary containing the data set names grouped per data tier,
    build the children dataset chain to include in the RAW dataset.

    Args:
        era (str): Dataset era being processed.
        era_data (dict): Relation between the PdmV campaign and the processing string
            compared.
        processing_string (str): Processing string for the data sets being grouped.
        group (dict): Data set names grouped by data tier. The structure is the following:
            {<data tier>: <dataset name>}, e.g: 
            {'DQMIO': '/BTagMu/CMSSW_12_4_11-124X_dataRun3_v11_gtval_RelVal_2022C-v1/DQMIO'}
    Returns:
        ChildDataset: Object that groups the relationship for the given datasets.
        None: If the given group doesn't contain the AOD datatiers.
    """
    def __retrieve_dataset_match(data_tier: str, dataset: str) -> DatasetMatch:
        """
        Build the match data object to retrieve the information.
        """
        requested: dict = era_data.get(data_tier, {})
        campaign: str = requested.get(
            "campaign", 
            f"<CampaignNotAvailableFor:{era}-{processing_string}>"
        )
        return DatasetMatch(
            campaign=campaign,
            processing_string=processing_string,
            dataset=dataset,
            data_tier=data_tier,
            era=era
        )


    # Check if the required datasets are included.
    REQUIRED_DATATIERS: set[str] = {'AOD'}
    if not REQUIRED_DATATIERS.intersection(group):
        logger.debug(
            (
                "Grouped datasets for processing string (%s) "
                "doesn't include the required data tiers: %s"
            ),
            processing_string,
            REQUIRED_DATATIERS
        )
        return None

    # Retrieve the data for the desired data tiers.
    children_relation: List[ChildDataset] = []
    retrieval_order: tuple = ("AOD", "MINIAOD", "NANOAOD")
    for data_tier in retrieval_order:
        dataset_name: Optional[str] = group.get(data_tier, "")
        if not dataset_name:
            break

        # Retrieve the data
        dataset_match: DatasetMatch = __retrieve_dataset_match(
            data_tier=data_tier,
            dataset=dataset_name
        )
        dataset_info: ChildDataset = __retrieve_dataset_info(dataset_requested=dataset_match)
        if dataset_info:
            children_relation.append(dataset_info)

    # Build the hierarchy
    if children_relation:
        first_child: ChildDataset = children_relation[0]
        current_child: ChildDataset = first_child
        for idx in range(1, len(children_relation)):
            children = children_relation[idx]
            current_child.output = [children]
            current_child = children
        
        return first_child

    return None


def match_era_datasets(
        raw_dataset: str,
        era: str,
        era_data: dict,
    ) -> List[ChildDataset]:
    """
    Queries the children datasets linked to a RAW datasets and filters
    them to match only those specified in the desired era

    Args:
        raw_dataset (str): RAW dataset name
        era (str): Desired era
        era_match (dict): Contains the campaigns and processing string desired for each data tier

    Returns:
        List[ChildDataset]: A list with all the children datasets for the
            given RAW dataset grouped by data tier.
    """
    children_datasets: List[str] = das_get_datasets_names(query=f"child dataset='{raw_dataset}'")
    
    # INFO: Beware, for some reason, NANOAOD datasets are not included into the children
    # relationship. Query for them via an extra query: 
    raw_primary_dataset, _, _ = das_retrieve_dataset_components(dataset=raw_dataset)
    nanoaod_query: str = f"/{raw_primary_dataset}/{era}*/NANOAOD"
    nanoaod_datasets: List[str] = das_get_datasets_names(query=f"dataset='{nanoaod_query}'")
    children_datasets += nanoaod_datasets
    children_datasets = sorted(children_datasets)

    # Group all children datasets per data tier and processing string
    children_grouped = {}
    for cd in children_datasets:
        _, processing_string, data_tier = das_retrieve_dataset_components(dataset=cd)
        if not children_grouped.get(processing_string):
            children_grouped[processing_string] = {}
        children_grouped[processing_string].update({data_tier: cd})

    children_per_processing: List[ChildDataset] = []
    for processing_str, grouped_data_tier in children_grouped.items():
        children_relation: Optional[ChildDataset] = __build_relationship(
            era=era,
            era_data=era_data,
            processing_string=processing_str,
            group=grouped_data_tier
        )
        if children_relation:
            children_per_processing.append(children_relation)

    return children_per_processing


def get_dataset_info(dataset: str, year_info: dict) -> dict:
    """
    For a RAW dataset, this function retrieves its metadata
    and all the sublevel datasets filtered by the interest campaigns and
    processing strings.

    Args:
        dataset (str): Name of the dataset to retrieve
        year_info (dict): Eras for the desired year

    Returns:
        dict: A dict containing the name, campaign, type (datatier),
            prepid, runs, events, workflow and processing string and all
            the data of the sublevel datatiers if they exist.
    """
    dataset_content: Optional[Tuple[dict, dict]] = das_get_dataset_info(dataset=dataset)
    if not dataset_content:
        logger.error("The requested dataset, %s, does not exist or its status is not valid or production", dataset)
        return None

    dataset_summary, _ = dataset_content
    events: int = dataset_summary.get("nevents", -1)
    runs: List[int] = das_get_runs(dataset=dataset)

    # Parse the dataset's year from the era attributes
    era = era_regex.search(string=dataset)[0]
    year = year_regex.search(string=era)[0]

    raw_dataset: RAWDataset = RAWDataset(
        dataset=dataset,
        events=events,
        year=year,
        runs=runs
    )

    # Retrieve the desired datasets for the era
    era_data: dict = year_info.get(era)
    if era_data:
        # Retrieve the sublevel datasets
        logger.info("Querying for the sublevel datasets for RAW dataset: %s" % dataset)
        interest_children_datasets: List[ChildDataset] = match_era_datasets(raw_dataset=dataset, era=era, era_data=era_data)
        if interest_children_datasets:
            raw_dataset.output = interest_children_datasets
        else:
            logger.error("No child datasets for RAW dataset: %s", dataset)
    else:
        logger.error("Unable to query the children data tier for RAW dataset: %s", dataset)
    
    return raw_dataset.dict


# Load dataset names using file
datasets = load_datasets_from_file(path="data/datasets.txt")
logger.info('Read %s datasets from file', len(datasets))

if '--debug' in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:10]
    logger.debug('Picking random %s datasets because debug', len(datasets))

datasets = sorted(datasets)
with open("data/years.json", "r", encoding="utf-8") as file:
    years = json.load(file)


results = []
breaker = 0
for index, raw_dataset in enumerate(datasets):
    print('%s/%s. Dataset is %s' % (index + 1, len(datasets), raw_dataset))
    for year, year_info in years.items():
        if '/Run%s' % (year) in raw_dataset:
            raw_dataset_data = get_dataset_info(dataset=raw_dataset, year_info=year_info.get("era"))
            if raw_dataset_data:
                results.append(raw_dataset_data)        
            break
    else:
        logger.error('***Could not find year info for %s ***', raw_dataset)
        continue


with open(f"{OUTPUT_FOLDER}/data.json", "w") as output_file:
    json.dump(results, output_file, indent=1, sort_keys=True)
