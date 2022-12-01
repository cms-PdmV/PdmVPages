from email.policy import default
import sys
import csv
import json
import os
import random
import re
import hashlib
import subprocess
import logging
from stats_rest import Stats2
from connection_wrapper import ConnectionWrapper
from typing import List, Tuple

stats = Stats2(cookie='stats-cookie.txt')
cmsweb = ConnectionWrapper('cmsweb.cern.ch', keep_open=True)
das_events_cache = {}
das_runs_cache = {}
das_events_of_runs_cache = {}
dataset_blacklist = ['/*/*DQMresub*/*AOD',
                     '/*/*EcalRecovery*/*AOD',
                     '/*/*WMass*/*AOD']
campaign_blacklist = ['NanoAODv6']

# Logger
# Set up logging
logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()

# Regex pattern for year
run_regex = re.compile(r"Run([0-9]{4})")
year_regex = re.compile(r"([0-9]{4})")

# Dataset version
dataset_version_regex = re.compile(r"(_v[0-9])?_v[0-9]|-v[0-9]")

# Constant
FILE_SUMMARY = "dbs3:filesummaries"
DATASET_INFO = "dbs3:dataset_info"
OUTPUT_FOLDER = f"{os.getcwd()}/output"


def make_regex_matcher(pattern):
    """
    Compile a regex pattern and return a function that performs fullmatch on
    given value
    """
    compiled_pattern = re.compile(pattern)

    def matcher_function(value):
        """
        Return whether given value fully matches the pattern
        """
        return compiled_pattern.fullmatch(value)

    return matcher_function


dataset_blacklist = [make_regex_matcher(x.replace('*', '.*')) for x in dataset_blacklist]


def is_dataset_in_blacklist(dataset_name):
    """
    Return whether given dataset is in blacklist
    """
    for ds_check in dataset_blacklist:
        if ds_check(dataset_name):
            return True

    return False


campaign_blacklist = [make_regex_matcher(x.replace('*', '.*')) for x in campaign_blacklist]


def is_campaign_in_blacklist(campaign):
    """
    Return whether given campaign is in blacklist
    """
    for ds_check in campaign_blacklist:
        if ds_check(campaign):
            return True

    return False


def chunkify(items, chunk_size):
    """
    Yield fixed size chunks of given list
    """
    start = 0
    chunk_size = max(chunk_size, 1)
    while start < len(items):
        yield items[start: start + chunk_size]
        start += chunk_size


def das_get_events(dataset):
    if not dataset:
        return 0

    if dataset in das_events_cache:
        return das_events_cache[dataset]

    result = 0
    try:
        result = int(os.popen('dasgoclient --query="dataset=' + dataset + ' | grep dataset.nevents"').read().strip())
        das_events_cache[dataset] = result
    except Exception as ex:
        print('Error getting events for %s: %s' % (dataset, str(ex)))

    return result


def das_get_events_of_runs(dataset, runs, try_to_chunkify=True):
    if not dataset or not runs:
        return 0

    if isinstance(runs, dict):
        runs = set(runs.keys())

    runs = sorted(list(runs))
    key = hashlib.sha256(('%s___%s' % (dataset, json.dumps(runs, sort_keys=True))).encode('utf-8')).hexdigest()
    if key in das_events_of_runs_cache:
        print('  Cache hit for %s, saved some time!' % (dataset))
        return das_events_of_runs_cache[key]

    try:
        print('  Getting events of %s runs of %s' % (len(runs), dataset))
        command = 'dasgoclient --query="file run in ' + str(list(runs)).replace(' ',
                                                                                '') + ' dataset=' + dataset + ' | sum(file.nevents)"'
        events = os.popen(command).read()
        events = int(float(events.split(' ')[-1]))
        print('  Got %s events' % (events))
        das_events_of_runs_cache[key] = events
        return events
    except:
        print('Error while getting events for %s with %s runs, trying to chunkify' % (dataset, len(runs)))
        if try_to_chunkify:
            events = 0
            for chunk in chunkify(runs, 50):
                events += das_get_events_of_runs(dataset, chunk, False)

            das_events_of_runs_cache[key] = events
            return events

    das_events_of_runs_cache[key] = 0
    return 0


def das_get_events_of_runs_lumis(dataset, runs):
    if not dataset or not runs:
        return 0

    key = hashlib.sha256(('%s___%s' % (dataset, json.dumps(runs, sort_keys=True))).encode('utf-8')).hexdigest()
    if key in das_events_of_runs_cache:
        print('  Cache hit for %s, saved some time!' % (dataset))
        return das_events_of_runs_cache[key]

    events_for_lumis = {}
    print('  Getting events of %s runs with lumis of %s' % (len(runs), dataset))
    for chunk in chunkify(sorted(list(runs)), 50):
        chunk_str = '[%s]' % (','.join([str(x) for x in chunk]))
        command = 'dasgoclient --query="file,run,lumi,events dataset=%s run in %s"' % (dataset, chunk_str)
        try:
            result = os.popen(command).read()
            result = [r.strip().split(' ')[1:] for r in result.split('\n') if r.strip()]
            for row in result:
                run = int(row[0])
                lumi_list = [int(x) for x in row[1].strip('[]').split(',')]
                if len(row) > 2 and row[2] != 'null':
                    event_list = [int(x) for x in row[2].strip('[]').split(',')]
                else:
                    # In case there is no info about lumis
                    event_list = [0] * len(lumi_list)

                for lumi, lumi_events in zip(lumi_list, event_list):
                    run_dict = events_for_lumis.setdefault(run, {})
                    run_dict[lumi] = max(run_dict.get(lumi, 0), lumi_events)
        except Exception as ex:
            print('Error in events of lumis for %s, chunk %s, command %s: %s' % (dataset, chunk_str, command, str(ex)))

    events = 0
    for run, lumi_ranges in runs.items():
        for lumi_range in lumi_ranges:
            for lumi in range(lumi_range[0], lumi_range[1] + 1):
                events += events_for_lumis.get(run, {}).get(lumi, 0)

    print('  Got %s events' % (events))
    das_events_of_runs_cache[key] = events
    return events


def das_get_runs(dataset):
    if not dataset:
        return []

    if dataset in das_runs_cache:
        return das_runs_cache[dataset]

    result = set()
    try:
        stream = os.popen('dasgoclient --query="run dataset=' + dataset + '"')
        result = set([int(r.strip()) for r in stream.read().split('\n') if r.strip()])
        print('    Got %s runs for %s' % (len(result), dataset))
        das_runs_cache[dataset] = result
    except Exception as ex:
        print('Error getting %s runs :%s' % (dataset, str(ex)))

    return result


def get_twiki_file(file_name):
    if not file_name:
        return []

    rows = []
    with open(file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            if row and not row[0].startswith('#'):
                rows.append(row)
    return rows


def get_dcs_json(file_name):
    if not file_name:
        return {}

    with open(file_name) as dcs_file:
        dcs_json = {int(run): lumis for run, lumis in json.load(dcs_file).items()}

    return dcs_json


def get_workflows_for_input(input_dataset):
    workflows = stats.get_input_dataset(input_dataset)
    workflows = [w for w in workflows if w['RequestType'].lower() not in ('resubmission', 'dqmharvest')]
    return workflows


def get_workflow(workflow_name):
    workflow = cmsweb.api('GET', '/couchdb/reqmgr_workload_cache/%s' % (workflow_name))
    workflow = json.loads(workflow)
    return workflow


def get_prepid_and_dataset(workflows, datatiers, year_dict):
    if not datatiers:
        return []

    results = []
    for workflow in workflows:
        latest_info = workflow['EventNumberHistory'][-1]
        for dataset_name, info in latest_info['Datasets'].items():
            if info['Type'] in ('PRODUCTION', 'VALID'):
                ds_datatier = dataset_name.split('/')[-1]
                if ds_datatier == datatiers[0]:  # Checking if it is an AOD, MINIAOD or NANOAOD
                    dataset_type = info['Type']
                    prepid = workflow['PrepID']
                    print('    Looking at %s (%s) of %s' % (dataset_name, dataset_type, prepid))
                    for res in results:
                        # Skip if we've already seen the dataset
                        if res['dataset'] == dataset_name:
                            break
                    else:
                        if is_dataset_in_blacklist(dataset_name):
                            print('      Skipping %s because it is blacklisted' % (dataset_name))
                            continue

                        processing_string = workflow['ProcessingString']
                        campaign = '<other>'
                        for campaign_name, campaign_tags in year_dict['campaigns'][ds_datatier].items():
                            if processing_string in campaign_tags:
                                campaign = campaign_name
                                break

                        if is_campaign_in_blacklist(campaign):
                            print('      Skipping %s because campaign is blacklisted' % (dataset_name))
                            continue

                        runs = das_get_runs(dataset_name)
                        item = {'dataset': dataset_name,
                                'campaign': campaign,
                                'type': dataset_type,
                                'prepid': prepid,
                                'runs': list(runs),
                                'events': das_get_events(dataset_name),  # Getting events from DAS and not Stats
                                'output': get_prepid_and_dataset([workflow], datatiers[1:], year_dict),
                                'workflow': workflow['RequestName'],
                                'processing_string': processing_string}
                        item['output'].extend(
                            get_prepid_and_dataset(get_workflows_for_input(dataset_name), datatiers[1:], year_dict))
                        results.append(item)

    return results


def parse_inject_processing_string(
        raw_dataset: str,
        processing_str: str,
        datatier: str,
) -> List[str]:
    """
    Creates the query to retrieve the dataset for a given processing string

    Parameters
    -----------------
    raw_dataset : str
        Dataset name without any processing string, ex: /BTagMu/Run2018A-v1/RAW
    processing_str : str
        Processing string to include in the query
    datatier : str in (AOD, MINIAOD, NANOAOD)
        Type of datatier to retrieve
    versions : List[int]
        Versions to retrieve from a processing strings if datatier is AOD. For example for datasets /Muon/Run2022D-PromptReco-, we have:
        /Muon/Run2022D-PromptReco-v1/AOD, /Muon/Run2022D-PromptReco-v2/AOD,
        /Muon/Run2022D-PromptReco-v3/AOD. This dataset has 3 versions: v1, v2, v3 so versions param would have a list
        with version numbers: [1, 2, 3].

    Returns
    ------------------
    List[str]
        Queries for retriving the datasets related with a specific processing string
    """
    token = [t for t in raw_dataset.split("/") if t]  # Split and delete empty strings
    primary_dataset = token[0]
    run_campaign = token[1].split("-")[0]
    dataset = f"/{primary_dataset}/{run_campaign}-{processing_str}*/{datatier}"
    result = os.popen('dasgoclient --query="dataset=' + dataset + ' | grep dataset.name"').read()
    list_result = [x.strip() for x in result.split("\n") if x]

    # Filter "custom and not welcome" tags
    final_filter = "(_v[0-9]-v[0-9]|-v[0-9])"
    dataset_final_filter = f"/{primary_dataset}/{run_campaign}-{processing_str}{final_filter}"
    dataset_final_regex = re.compile(dataset_final_filter)
    list_result = [x for x in list_result if dataset_final_regex.search(x)]

    return list_result


def das_get_dataset_info(dataset: str):
    """
    Retrieves the dataset info and its sumary from DAS

    Parameters
    -----------------
    dataset : str
        Name of the dataset to retrieve

    Returns
    ------------------
    Tuple[dict, dict] | None
        If the dataset has files and its status is valid or production. The first dict is the db3:filesummary -> dataset
        object and the second dict is the db3:dataset_info -> dataset object. Returns None if the dataset does not have files
        or if it is not valid.
    """
    # Return type:  -> Tuple[dict, dict] | None
    # Return file summary and dataset info if dataset is VALID. Else, return None.
    command = "dasgoclient --query=%s --json" % dataset
    result = subprocess.run(command.split(" "), stdout=subprocess.PIPE)
    json_result = json.loads(result.stdout)

    # Get the db3:filesummary and "dbs3:dataset_info"
    file_summary = {}
    dataset_info = {}
    for obj in json_result:
        if obj["das"]["services"][0] == FILE_SUMMARY:
            file_summary = obj["dataset"][0]
        elif obj["das"]["services"][0] == DATASET_INFO:
            dataset_info = obj["dataset"][0]

    # Check if it has files and events
    if dataset_info.get("status") in ("PRODUCTION", "VALID") and file_summary.get("nfiles") > 0:
        return file_summary, dataset_info

    return None


def get_dataset_version(dataset_name: str):
    version = dataset_version_regex.search(dataset_name)
    if version:
        version_match = version[0]
        version_str = version_match[version_match.index("v") + 1]
        return int(version_str)
    return 0


def get_dataset_steps(dataset: str, datatiers: List[str], year_dict: dict, parent_dataset: str = None) -> dict:
    """
    For a RAW dataset, this function retrieves the all the datatiers and processing strings required

    Parameters
    -----------------
    dataset : str
        Name of the dataset to retrieve
    datatiers : List[str]
        List of datatiers to retrieve
    year_dict : dict
        List of all year, campaigns and processing strings to retrieve for a given RAW dataset

    Returns
    ------------------
    dict
        A dict containing the name, campaign, type (datatier), prepid, runs, events, workflow and processing string and all
        the data of the sublevel datatiers if they exist.
    """
    # Base case: There are not datatiers to query -> AOD, MINIAOD, .....
    if not datatiers:
        print("[get_dataset_steps] Base case reached for dataset: ", dataset)
        return []

    current_datatier = datatiers[0]
    # Take all the subcampaings for a group
    datatier_campaigns = year_dict["campaigns"][current_datatier]

    results = []
    for campaign, processing_str_list in datatier_campaigns.items():
        # Only use the lastest processing string from the config file
        processing_str = processing_str_list[-1]

        # Create the queries for retrieving datasets
        dataset_queries: List[str] = parse_inject_processing_string(
            raw_dataset=dataset, processing_str=processing_str,
            datatier=current_datatier
        )

        # Filter queries, make sure they are related to the same version
        if parent_dataset:
            parent_dataset_version = get_dataset_version(parent_dataset)
            dataset_queries = [
                d
                for d in dataset_queries
                if get_dataset_version(d) == parent_dataset_version
            ]

        # Iterate over all versions
        for dataset_query in dataset_queries:
            print(f"[get_dataset_steps] Dataset for querying: {dataset_query}")
            # Get dataset info
            dataset_result = das_get_dataset_info(dataset=dataset_query)
            if dataset_result is None:
                # Dataset is not valid
                print(f"[get_dataset_steps] Dataset not valid: {dataset_query}")
                break

            # Unpackage objects
            file_summary, dataset_info = dataset_result

            # Define the parent_dataset
            parent_dataset = dataset_query

            # Package dataset data
            runs = das_get_runs(dataset=dataset_query)
            next_datatier = datatiers[1:]
            print(f"[get_dataset_steps] Dataset: {dataset} - Recursive case: Next datatiers to check: {next_datatier}")
            item = {
                'dataset': dataset_info["name"],
                'campaign': campaign,
                'type': dataset_info["status"],
                'prepid': None,  # Taken using DAS
                'runs': list(runs),
                'events': file_summary["nevents"],  # Getting events from DAS and not Stats
                'output': get_dataset_steps(dataset, datatiers[1:], year_dict, parent_dataset=parent_dataset),  # Recursive case
                'workflow': None,  # Taken using DAS
                'processing_string': processing_str
            }

            # Agregar el elemento
            results.append(item)

    return results


def get_dataset_info(dataset: str) -> dict:
    """
    For a RAW dataset, this function retrieves its metadata and all the sublevel datasets filtered by the interest campaigns and
    processing strings.

    Parameters
    -----------------
    dataset : str
        Name of the dataset to retrieve

    Returns
    ------------------
    dict
        A dict containing the name, campaign, type (datatier), prepid, runs, events, workflow and processing string and all
        the data of the sublevel datatiers if they exist.
    """
    events = das_get_events(dataset=dataset)
    runs = list(das_get_runs(dataset=dataset))
    run_name = run_regex.search(string=dataset)[0]
    year = year_regex.search(string=run_name)[0]

    # Get all AOD, MINIAOD and NANOAOD datasets
    print(f"[get_dataset_info] Querying for dataset datatiers using function [get_dataset_info] -> Dataset: {dataset}")
    aod_datasets: dict = get_dataset_steps(dataset, ['AOD', 'MINIAOD', 'NANOAOD'], year_info)
    item = {
        'dataset': dataset,
        'output': aod_datasets,
        'events': events,
        'twiki_runs': [],
        'year': year,
        'runs': runs
    }
    return item


def load_datasets_from_file(path: str) -> list:
    """
    Load all the raw dataset name to be processed.

    Parameters
    -----------------
    path : str
        Path to the text file with all the names

    Returns
    ------------------
    list
        All raw dataset names to be processed
    """
    with open(path) as datasets_file:
        dataset_list = list(set([d.strip() for d in datasets_file.read().split('\n') if d.strip()]))
    return dataset_list


def das_get_datasets_names(query: str):
    """
    Given a query, retrieve from DAS all dataset name that fulfill the condition.

    Parameters
    -----------------
    query : str
        Dataset condition to retrieve.

    Returns
    ------------------
    list
        All dataset name that fulfill the condition.
    """
    command = "dasgoclient --query=%s --json" % query
    result = subprocess.run(command.split(" "), stdout=subprocess.PIPE)
    json_result = json.loads(result.stdout)
    dataset_name_list = [e["dataset"][0]["name"] for e in json_result]
    return dataset_name_list


def das_get_all_datasets_names_year(years: list):
    """
    Get all raw datasets names on a year.

    Parameters
    -----------------
    years : List[str]
        All years to query to retrieve datasets

    Returns
    ------------------
    list
        All raw dataset names that fulfill the condition.
    """
    result = []
    for y in years:
        query = "dataset=/*/Run%s*/RAW" % y
        result += das_get_datasets_names(query=query)
    return result


def load_json(path: str) -> dict:
    """
    Get all raw datasets names on a year.

    Parameters
    -----------------
    years : List[str]
        All years to query to retrieve datasets

    Returns
    ------------------
    list
        All raw dataset names that fulfill the condition.
    """
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data


def filter_output_datasets(results: list, subset: list):
    """
    Filter the result dataset only to keep a subset of datasets

    Parameters
    -----------------
    results : List[dict]
        A list with all RAW, AOD, MINIAOD and NANOAOD data for each available primary dataset
        in DAS.
    results : List[str]
        A list with a primary dataset subset to filter. This list only includes RAW dataset names.


    Returns
    ------------------
    List[dict]
        Dataset list filtered
    """
    subset_result = []
    for r in results:
        if r["dataset"] in subset:
            subset_result.append(r)
    return subset_result


# Load dataset names using file
datasets_subset = load_datasets_from_file(path="data/datasets.txt")

# Load the names of the interest datasets
datasets = das_get_all_datasets_names_year(years=["2022"])
print('Read %s datasets from file' % (len(datasets)))

if '--debug' in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:10]
    print('Picking random %s datasets because debug' % (len(datasets)))

datasets = sorted(datasets)
print(f"RAW Datasets for year 2022: {datasets} \n")

years = load_json("data/years.json")
# print("Year JSON data: ", years)

for year, year_info in years.items():
    year_info['twiki_file'] = get_twiki_file(year_info['twiki_file_name'])
    year_info['dcs_json'] = get_dcs_json(year_info['dcs_json_path'])


results = []
breaker = 0
for index, raw_dataset in enumerate(datasets):
    print('%s/%s. Dataset is %s' % (index + 1, len(datasets), raw_dataset))
    for year, year_info in years.items():
        if '/Run%s' % (year) in raw_dataset:
            break
    else:
        print('***Could not find year info for %s ***' % (raw_dataset))
        continue

    results.append(get_dataset_info(raw_dataset))

results_subset = filter_output_datasets(results=results, subset=datasets_subset)

with open(f"{OUTPUT_FOLDER}/data.json", "w") as output_file:
    print("[Main] Saving JSON: ", results)
    json.dump(results, output_file, indent=1, sort_keys=True)

with open(f"{OUTPUT_FOLDER}/data_subset.json", "w") as output_file:
    print("[Main] Saving Subset JSON: ", results_subset)
    json.dump(results_subset, output_file, indent=1, sort_keys=True)
