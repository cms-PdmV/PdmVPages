import json
import time
import os

OUTPUT_FOLDER = f"{os.getcwd()}/output"


def pick_output_item(items, campaign, processing_string=None):
    if not items:
        return {}

    for dataset_type in ('VALID', 'PRODUCTION'):
        for item in items:
            if item['type'] == dataset_type and item['campaign'] == campaign:
                if 'LowPU' in item['processing_string']:
                    # Because in this table we _explicitly_ don't care about LowPU requests
                    continue

                if not processing_string or processing_string == item['processing_string']:
                    return item

    return {}


with open(f'{OUTPUT_FOLDER}/data.json', 'r') as data_file:
    items = json.load(data_file)

print('Read %s items from data.json' % (len(items)))

results = []
for item in items:
    raw_dataset = item['dataset']
    print('Dataset %s' % (raw_dataset))
    if 'Run2016F' in raw_dataset:
        # HIPM and non-HIPM for 2016F
        aods = [pick_output_item(item.get('output'), 'AOD', '21Feb2020_UL2016'),
                pick_output_item(item.get('output'), 'AOD', '21Feb2020_UL2016_HIPM')]
    else:
        aods = [pick_output_item(item.get('output'), 'AOD')]

    for aod in aods:
        miniaod_v3 = pick_output_item(aod.get('output'), 'MiniAODv3')
        nanoaod_v10 = pick_output_item(miniaod_v3.get('output'), 'NanoAODv10')

        if aod:
            print('  AOD: %s (%s)' % (aod['dataset'], aod['type']))

        if miniaod_v3:
            print('  MiniAODv3: %s (%s)' % (miniaod_v3['dataset'], miniaod_v3['type']))

        if nanoaod_v10:
            print('    NanoAODv10: %s (%s)' % (nanoaod_v10['dataset'], nanoaod_v10['type']))

        twiki_runs = set(item['twiki_runs'])
        raw_runs = set(item.get('runs', []))
        raw_events = item.get('events', 0)
        raw_x_dcs_runs = set(aod.get('raw_x_dcs_runs', []))
        raw_x_dcs_events = aod.get('raw_x_dcs_events', 0)
        whitelist_x_raw_runs = set(aod.get('whitelist_x_raw_runs', []))
        whitelist_x_raw_events = aod.get('whitelist_x_raw_events', 0)
        whitelist_x_raw_x_dcs_runs = set(aod.get('whitelist_x_raw_x_dcs_runs', []))
        whitelist_x_raw_x_dcs_events = aod.get('whitelist_x_raw_x_dcs_events', 0)

        result = {'input_dataset': raw_dataset,
                  'year': item['year'],
                  'primary_dataset': raw_dataset.split('/')[1],
                  'twiki_runs': len(twiki_runs),

                  'raw_runs': len(raw_runs),
                  'raw_events': raw_events,
                  'raw_x_dcs_runs': len(raw_x_dcs_runs),
                  'raw_x_dcs_events': raw_x_dcs_events,
                  'whitelist_x_raw_runs': len(whitelist_x_raw_runs),
                  'whitelist_x_raw_events': whitelist_x_raw_events,
                  'whitelist_x_raw_x_dcs_runs': len(whitelist_x_raw_x_dcs_runs),
                  'whitelist_x_raw_x_dcs_events': whitelist_x_raw_x_dcs_events,

                  'twiki_and_whitelist_x_raw_missing_runs': len(whitelist_x_raw_runs - twiki_runs),
                  'twiki_and_whitelist_x_raw_surplus_runs': len(twiki_runs - whitelist_x_raw_runs),
                  'twiki_and_whitelist_x_raw_runs_diff': len(whitelist_x_raw_runs - twiki_runs) + len(twiki_runs - whitelist_x_raw_runs),

                  'whitelist_x_raw_and_raw_x_dcs_missing_runs': len(raw_x_dcs_runs - whitelist_x_raw_runs),
                  'whitelist_x_raw_and_raw_x_dcs_surplus_runs': len(whitelist_x_raw_runs - raw_x_dcs_runs),
                  'whitelist_x_raw_and_raw_x_dcs_runs': len(raw_x_dcs_runs - whitelist_x_raw_runs) + len(whitelist_x_raw_runs - raw_x_dcs_runs),
                  }

        for prefix, (thing, parent) in {'aod': (aod, item),
                                        'miniaod_v3': (miniaod_v3, aod),
                                        'nanoaod_v10': (nanoaod_v10, miniaod_v3),
                                        }.items():
            dataset = thing.get('dataset')
            result[prefix + '_dataset'] = dataset
            result[prefix + '_dataset_status'] = thing.get('type')
            result[prefix + '_prepid'] = thing.get('prepid')
            runs = set(thing.get('runs', []))
            events = thing.get('events', 0)
            result[prefix + '_runs'] = len(runs)
            result[prefix + '_events'] = events
            result[prefix + '_produced_vs_parent_ratio'] = None
            result[prefix + '_vs_parent_missing_runs'] = None
            result[prefix + '_vs_parent_surplus_runs'] = None
            result[prefix + '_vs_parent_runs_diff'] = None
            if parent:
                result[prefix + '_produced_vs_parent_ratio'] = float(events) / parent['events'] if parent and parent['events'] else None
                result[prefix + '_vs_parent_missing_runs'] = len(set(parent.get('runs', [])) - runs)
                result[prefix + '_vs_parent_surplus_runs'] = len(runs - set(parent.get('runs', [])))
                result[prefix + '_vs_parent_runs_diff'] = result[prefix + '_vs_parent_missing_runs'] + result[prefix + '_vs_parent_surplus_runs']
            elif dataset:
                result[prefix + '_produced_vs_parent_ratio'] = float(events) / whitelist_x_raw_events if whitelist_x_raw_events else None
                result[prefix + '_vs_parent_missing_runs'] = len(whitelist_x_raw_runs - runs)
                result[prefix + '_vs_parent_surplus_runs'] = len(runs - whitelist_x_raw_runs)
                result[prefix + '_vs_parent_runs_diff'] = result[prefix + '_vs_parent_missing_runs'] + result[prefix + '_vs_parent_surplus_runs']

        results.append(result)


with open(f'{OUTPUT_FOLDER}/data_original_table.json', 'w') as output_file:
    json.dump(results, output_file, indent=1, sort_keys=True)

with open(f'{OUTPUT_FOLDER}/original_table_timestamp.txt', 'w') as output_file:
    output_file.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
