"""
This module implements some functions to
load the content for JSON files and Text files
"""
import csv
import json
from typing import List

def load_datasets_from_file(path: str) -> List[str]:
    """
    Load all the raw dataset names to be processed.

    Args:
        path (str): Path to the text file with all the names

    Returns:
        list[str]: All raw dataset names loaded from the file
    """
    with open(path) as datasets_file:
        dataset_list = list(set([d.strip() for d in datasets_file.read().split('\n') if d.strip()]))
    return dataset_list


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