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


def match_era_datasets(raw_dataset: str, era: str, era_data: dict, lax: bool = True) -> EraDatasets:
    """
    Queries the children datasets linked to a RAW datasets and filters
    them to match only those specified in the desired era

    Args:
        raw_dataset (str): RAW dataset name
        era (str): Desired era
        era_match (dict): Contains the campaigns and processing string desired for each data tier
        lax (bool): If the desired processing string is not available for a datatier,
            this will include the most recent available dataset for the data tier

    Returns:
        EraDatasets: The group of dataset that matches the specified campaign
            and processing string for each data tier
    """

    def __lax_warning_message__(original_ps: str, available_ps, datatier: str):
        """
        Record a warning message to alert the user the requested dataset was replaced
        due to the lax condition
        """
        logger.warning(
            (
                "Using lax mode! "
                "This relaxes the restrictions for retriving datasets using an exact "
                "processing string. \n"
                "It was not possible to retrieve a dataset for the processing string: %s \n"
                "Instead, we are setting a dataset with the processing string: %s \n"
                "Data tier: %s"
            ), 
            original_ps,
            available_ps,
            datatier
        )


    children_datasets: List[str] = das_get_datasets_names(query=f"child dataset='{raw_dataset}'")
    
    # INFO: Be aware, for some reason, NANOAOD datasets are not included into the children
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
        if not children_grouped.get(data_tier):
            children_grouped[data_tier] = {}
        children_grouped[data_tier].update({processing_string: cd})

    # Are AOD data sets available?
    aod_found: dict = children_grouped.get("AOD")
    aod_requested: dict = era_data.get("AOD")
    aod_processing_string: str = aod_requested.get("processing_string")

    if not aod_found:
        # There is no dataset available for this data tier.
        logger.error("There is no AOD data set available for the RAW data set: %s",
            raw_dataset
        )
        return EraDatasets(
            era=era,
            aod=None,
            miniaod=None,
            nanoaod=None
        )

    if not aod_found.get(aod_processing_string):
        if not lax:
            # There is no dataset available for this processing string and data tier.
            logger.error("There is no AOD data set available for the RAW data set: %s and the processing string: %s",
                raw_dataset, aod_processing_string
            )
            return EraDatasets(
                era=era,
                aod=None,
                miniaod=None,
                nanoaod=None
            )
        else:
            # Choose the most recent dataset available for the category
            aod_dataset_names: List[str] = aod_found.values()
            aod_info: DatasetMatch = retrieve_latest_dataset(
                datasets=aod_dataset_names,
                campaign=aod_requested.get("campaign"),
                era=era
            )
            __lax_warning_message__(aod_processing_string, aod_info.processing_string, aod_info.data_tier)
    else:
        aod_info: DatasetMatch = DatasetMatch(
            campaign=aod_requested.get("campaign"),
            processing_string=aod_processing_string,
            dataset=aod_found.get(aod_processing_string),
            data_tier="AOD",
            era=era
        )

    # Are MiniAOD data sets available?
    miniaod_found: dict = children_grouped.get("MINIAOD")
    miniaod_requested: dict = era_data.get("MINIAOD")
    miniaod_processing_string: str = miniaod_requested.get("processing_string")

    if not miniaod_found:
        # There is no dataset available for this data tier.
        logger.error("There is no MiniAOD data set available for the RAW data set: %s",
            raw_dataset
        )
        return EraDatasets(
            era=era,
            aod=aod_info,
            miniaod=None,
            nanoaod=None
        )

    if not miniaod_found.get(miniaod_processing_string):
        if not lax:
            # There is no dataset available for this processing string and data tier.
            logger.error("There is no MiniAOD data set available for the RAW data set: %s and the processing string: %s",
                raw_dataset, miniaod_processing_string
            )
            return EraDatasets(
                era=era,
                aod=aod_info,
                miniaod=None,
                nanoaod=None
            )
        else:
            # Choose the most recent dataset available for the category
            miniaod_dataset_names: List[str] = miniaod_found.values()
            miniaod_info: DatasetMatch = retrieve_latest_dataset(
                datasets=miniaod_dataset_names,
                campaign=miniaod_requested.get("campaign"),
                era=era
            )
            __lax_warning_message__(miniaod_processing_string, miniaod_info.processing_string, miniaod_info.data_tier)
    else:
        miniaod_info: DatasetMatch = DatasetMatch(
            campaign=miniaod_requested.get("campaign"),
            processing_string=miniaod_processing_string,
            dataset=miniaod_found.get(miniaod_processing_string),
            data_tier="MINIAOD",
            era=era
        )

    # Are NanoAOD data sets available?
    nanoaod_found: dict = children_grouped.get("NANOAOD")
    nanoaod_requested: dict = era_data.get("NANOAOD")
    nanoaod_processing_string: str = nanoaod_requested.get("processing_string")

    if not nanoaod_found:
        # There is no dataset available for this data tier.
        logger.error("There is no NanoAOD data set available for the RAW data set: %s",
            raw_dataset
        )
        return EraDatasets(
            era=era,
            aod=aod_info,
            miniaod=miniaod_info,
            nanoaod=None
        )

    if not nanoaod_found.get(nanoaod_processing_string):
        if not lax:
            # There is no dataset available for this processing string and data tier.
            logger.error("There is no NanoAOD data set available for the RAW data set: %s and the processing string: %s",
                raw_dataset, nanoaod_processing_string
            )
            return EraDatasets(
                era=era,
                aod=aod_info,
                miniaod=miniaod_info,
                nanoaod=None
            )
        else:
            # Choose the most recent dataset available for the category
            nanoaod_dataset_names: List[str] = nanoaod_found.values()
            nanoaod_info: DatasetMatch = retrieve_latest_dataset(
                datasets=nanoaod_dataset_names,
                campaign=nanoaod_requested.get("campaign"),
                era=era
            )
            __lax_warning_message__(nanoaod_processing_string, nanoaod_info.processing_string, nanoaod_info.data_tier)
    else:
        nanoaod_info: DatasetMatch = DatasetMatch(
            campaign=nanoaod_requested.get("campaign"),
            processing_string=nanoaod_processing_string,
            dataset=nanoaod_found.get(nanoaod_processing_string),
            data_tier="NANOAOD",
            era=era
        )

    return EraDatasets(
        era=era,
        aod=aod_info,
        miniaod=miniaod_info,
        nanoaod=nanoaod_info,
    )


def __retrieve_dataset_info__(dataset_requested: DatasetMatch) -> ChildDataset:
    """
    Retrives the dataset information from DAS for a desired dataset and parses it
    into the required format to display this information into both tables.

    Args:
        dataset_requested (DatasetMatch): Dataset to query

    Returns:
        ChildDataset: Dataset information parsed into the required format
    """
    summary, info = das_get_dataset_info(dataset=dataset_requested.dataset)
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



def retrieve_children_datasets(children_datasets: EraDatasets) -> Optional[ChildDataset]:
    """
    Retrieves the data related to the sublevel (children) data tiers linked for a RAW dataset
    and sets the hierarchical relationship between data tiers.

    Args:
        children_datasets (EraDatasets): The sublevel datasets to retrieve for each data tier

    Returns:
        ChildDataset | None: The AOD dataset information linked to a RAW dataset containing
            data for the MINIAOD and NANOAOD datasets associated with it.
            None if there no AOD dataset yet for this RAW dataset
    """
    # Retrieve the dataset info for each data tier
    aod_info: Optional[ChildDataset] = __retrieve_dataset_info__(dataset_requested=children_datasets.aod) if children_datasets.aod else None
    miniaod_info: Optional[ChildDataset] = __retrieve_dataset_info__(dataset_requested=children_datasets.miniaod) if children_datasets.miniaod else None
    nanoaod_info: Optional[ChildDataset] = __retrieve_dataset_info__(dataset_requested=children_datasets.nanoaod) if children_datasets.nanoaod else None

    # Set the hierarchical order
    if miniaod_info and nanoaod_info:
        miniaod_info.output = [nanoaod_info]
        aod_info.output = [miniaod_info]
        return aod_info
    elif aod_info and miniaod_info:
        aod_info.output = [miniaod_info]
        return aod_info
    return aod_info


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
        interest_children_datasets: EraDatasets = match_era_datasets(raw_dataset=dataset, era=era, era_data=era_data)
        raw_children_datasets: ChildDataset = retrieve_children_datasets(children_datasets=interest_children_datasets)
        if raw_children_datasets:
            raw_dataset.output = [raw_children_datasets]
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
