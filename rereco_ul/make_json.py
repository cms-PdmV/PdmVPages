import sys
import csv
import json
import os
import random
import re
from stats_rest import Stats2
from connection_wrapper import ConnectionWrapper


stats = Stats2(cookie='stats-cookie.txt')
cmsweb = ConnectionWrapper('cmsweb.cern.ch', keep_open=True)
das_events_cache = {}
das_runs_cache = {}
das_events_of_runs_cache = {}
dataset_blacklist = ['/*/*DQMresub*/*AOD',
                     '/*/*EcalRecovery*/*AOD',
                     '/*/*WMass*/*AOD']
campaign_blacklist = ['NanoAODv6']

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

    result = int(os.popen('dasgoclient --query="dataset=' + dataset + ' | grep dataset.nevents"').read().strip())
    das_events_cache[dataset] = result
    return result


def das_get_events_of_runs(dataset, runs, try_to_chunkify=True):
    if not dataset or not runs:
        return 0

    runs = sorted(list(runs))
    key = '%s___%s' % (','.join([str(x) for x in runs]), dataset)
    if key in das_events_of_runs_cache:
        return das_events_of_runs_cache[key]

    try:
        print('  Getting events of %s runs of %s' % (len(runs), dataset))
        command = 'dasgoclient --query="file run in ' + str(list(runs)).replace(' ', '') + ' dataset=' + dataset + ' | sum(file.nevents)"'
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


def das_get_runs(dataset):
    if not dataset:
        return []

    if dataset in das_runs_cache:
        return das_runs_cache[dataset]

    stream = os.popen('dasgoclient --query="run dataset=' + dataset + '"')
    result = set([int(r.strip()) for r in stream.read().split('\n') if r.strip()])
    das_runs_cache[dataset] = result
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


def get_dcs_json_runs(file_name):
    if not file_name:
        return []

    with open(file_name) as dcs_file:
        runs = [int(x) for x in json.load(dcs_file).keys()]

    return runs


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
                if ds_datatier == datatiers[0]:
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
                        item['output'].extend(get_prepid_and_dataset(get_workflows_for_input(dataset_name), datatiers[1:], year_dict))
                        results.append(item)

    return results


with open('datasets.txt') as datasets_file:
    datasets = list(set([d.strip() for d in datasets_file.read().split('\n') if d.strip()]))


print('Read %s datasets from file' % (len(datasets)))
if '--debug' in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:10]
    print('Picking random %s datasets because debug' % (len(datasets)))

datasets = sorted(datasets)

years = {'2016': {'twiki_file_name': '2016ULdataFromTwiki.txt',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/DCSOnly/json_DCSONLY.txt',
                  'campaigns': {'AOD':     {'AOD':       ['21Feb2020_UL2016', '21Feb2020_UL2016_HIPM', '21Feb2020_UL2016_HIPM_rsb',   '21Feb2020_ver2_UL2016_HIPM', '21Feb2020_ver1_UL2016_HIPM']},
                                'MINIAOD': {'MiniAODv1': ['21Feb2020_UL2016', '21Feb2020_UL2016_HIPM', '21Feb2020_UL2016_HIPM_rsb',   '21Feb2020_ver2_UL2016_HIPM', '21Feb2020_ver1_UL2016_HIPM'],
                                            'MiniAODv2': ['UL2016_MiniAODv2', 'HIPM_UL2016_MiniAODv2', 'ver1_HIPM_UL2016_MiniAODv2', 'ver2_HIPM_UL2016_MiniAODv2']},
                                'NANOAOD': {'NanoAODv6': ['Nano02Dec2019', 'Nano02Dec2019_21Feb2020_UL2016', 'Nano02Dec2019_21Feb2020_UL2016_HIPM'],
                                            'NanoAODv8': ['UL2016_MiniAODv1_NanoAODv2', 'HIPM_UL2016_MiniAODv1_NanoAODv2', 'ver1_HIPM_UL2016_MiniAODv1_NanoAODv2', 'ver2_HIPM_UL2016_MiniAODv1_NanoAODv2'],
                                            'NanoAODv9': ['UL2016_MiniAODv2_NanoAODv9', 'HIPM_UL2016_MiniAODv2_NanoAODv9', 'ver1_HIPM_UL2016_MiniAODv2_NanoAODv9', 'ver2_HIPM_UL2016_MiniAODv2_NanoAODv9']}}},
         '2017': {'twiki_file_name': '2017ULdataFromTwiki.txt',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/DCSOnly/json_DCSONLY.txt',
                  'campaigns': {'AOD':     {'AOD':       ['09Aug2019_UL2017_rsb', '09Aug2019_UL2017', '09Aug2019_UL2017_LowPU']},
                                'MINIAOD': {'MiniAODv1': ['09Aug2019_UL2017_rsb', '09Aug2019_UL2017', '09Aug2019_UL2017_LowPU'],
                                            'MiniAODv2': ['UL2017_MiniAODv2']},
                                'NANOAOD': {'NanoAODv6': ['Nano02Dec2019', 'UL2017_02Dec2019', 'UL2017_Nano02Dec2019', 'UL2017_Nano02Dec2019_rsb'],
                                            'NanoAODv8': ['UL2017_MiniAODv1_NanoAODv2', 'UL2017_LowPU_MiniAODv1_NanoAODv2'],
                                            'NanoAODv9': ['UL2017_MiniAODv2_NanoAODv9']}}},
         '2018': {'twiki_file_name': '2018ULdataFromTwiki.txt',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/DCSOnly/json_DCSONLY.txt',
                  'campaigns': {'AOD':     {'AOD':       ['12Nov2019_UL2018_rsb_v3', '12Nov2019_UL2018_rsb_v2', '12Nov2019_UL2018_rsb', '12Nov2019_UL2018', '12Nov2019_UL2018_LowPU']},
                                'MINIAOD': {'MiniAODv1': ['12Nov2019_UL2018_rsb_v3', '12Nov2019_UL2018_rsb_v2', '12Nov2019_UL2018_rsb', '12Nov2019_UL2018', '12Nov2019_UL2018_LowPU'],
                                            'MiniAODv2': ['UL2018_MiniAODv2']},
                                'NANOAOD': {'NanoAODv6': ['Nano02Dec2019', 'Nano02Dec2019_12Nov2019_UL2018', 'Nano02Dec2019_12Nov2019_UL2018_rsb', 'Nano02Dec2019_12Nov2019_UL2018_rsb_v2'],
                                            'NanoAODv8': ['UL2018_MiniAODv1_NanoAODv2'],
                                            'NanoAODv9': ['UL2018_MiniAODv2_NanoAODv9']}}}}


exception_2016F_HIPM_runs = set([277932, 277934, 277981, 277991, 277992, 278017,
                                 278018, 278167, 278175, 278193, 278239, 278240,
                                 278273, 278274, 278288, 278289, 278290, 278308,
                                 278309, 278310, 278315, 278345, 278346, 278349,
                                 278366, 278406, 278509, 278761, 278770, 278806,
                                 278807])
exception_2016F_nonHIPM_runs = set([278769, 278801, 278802, 278803,
                                    278804, 278805, 278808])

for year, year_info in years.items():
    year_info['twiki_file'] = get_twiki_file(year_info['twiki_file_name'])
    year_info['dcs_json'] = get_dcs_json_runs(year_info['dcs_json_path'])


results = []
for index, raw_dataset in enumerate(datasets):
    print('%s/%s. Dataset is %s' % (index + 1, len(datasets), raw_dataset))
    for year, year_info in years.items():
        if '/Run%s' % (year) in raw_dataset:
            break
    else:
        print('  ***Could not find year info for %s ***' % (raw_dataset))
        continue

    # Year
    print('  Year is %s' % (year))
    aod_tags = year_info['campaigns']['AOD']['AOD']
    dcs_runs = year_info['dcs_json']
    # TWiki row
    twiki_runs = set()
    for row in year_info['twiki_file']:
        if row[0] == raw_dataset:
           twiki_runs = set([int(x.strip()) for x in row[2].strip('[]').split(',') if x.strip()])
           break

    print('  Twiki runs: %s' % (len(twiki_runs)))
    # RAW dataset
    raw_input_workflows = get_workflows_for_input(raw_dataset)
    raw_input_workflows = [w for w in raw_input_workflows if [tag for tag in aod_tags if tag in w['ProcessingString']]]
    raw_events = das_get_events(raw_dataset)
    raw_runs = das_get_runs(raw_dataset)
    raw_x_dcs_runs = set(raw_runs).intersection(set(dcs_runs))
    # AOD, MiniAOD, NanoAOD
    aod_workflows = get_prepid_and_dataset(raw_input_workflows, ['AOD', 'MINIAOD', 'NANOAOD'], year_info)
    item = {'dataset': raw_dataset,
            'output': aod_workflows,
            'events': raw_events,
            'twiki_runs': list(twiki_runs),
            'year': year,
            'runs': list(raw_runs)}

    for aod_item in aod_workflows:
        whitelist_runs = set(get_workflow(aod_item['workflow'])['RunWhitelist'])
        whitelist_x_dcs_runs = whitelist_runs.intersection(set(dcs_runs))
        whitelist_x_raw_runs = whitelist_runs.intersection(set(raw_runs))
        whitelist_x_raw_x_dcs_runs = whitelist_x_raw_runs.intersection(set(dcs_runs))
        raw_x_dcs_runs_copy = set(raw_x_dcs_runs)
        if '2016F' in raw_dataset:
            if 'HIPM' in aod_item['processing_string']:
                # HIPM
                raw_x_dcs_runs_copy -= exception_2016F_nonHIPM_runs
                whitelist_x_raw_runs -= exception_2016F_nonHIPM_runs
                whitelist_x_raw_x_dcs_runs -= exception_2016F_nonHIPM_runs
            else:
                # non-HIPM
                raw_x_dcs_runs_copy -= exception_2016F_HIPM_runs
                whitelist_x_raw_runs -= exception_2016F_HIPM_runs
                whitelist_x_raw_x_dcs_runs -= exception_2016F_HIPM_runs

        aod_item['raw_x_dcs_runs'] = list(raw_x_dcs_runs_copy)
        aod_item['raw_x_dcs_events'] = das_get_events_of_runs(raw_dataset, aod_item['raw_x_dcs_runs'])
        aod_item['whitelist_runs'] = list(whitelist_runs)
        aod_item['whitelist_events'] = das_get_events_of_runs(raw_dataset, aod_item['whitelist_runs'])
        aod_item['whitelist_x_dcs_runs'] = list(whitelist_x_dcs_runs)
        aod_item['whitelist_x_dcs_events'] = das_get_events_of_runs(raw_dataset, aod_item['whitelist_x_dcs_runs'])

    results.append(item)


with open('data.json', 'w') as output_file:
    json.dump(results, output_file, indent=1, sort_keys=True)
