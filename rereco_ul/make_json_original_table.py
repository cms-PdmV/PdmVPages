import json
import time


def pick_output_item(items, campaign, processing_string=None):
    if not items:
        return {}

    for dataset_type in ('VALID', 'PRODUCTION'):
        for item in reversed(sorted(items, key=lambda x: '_'.join(x['workflow'].split('_')[-3:]))):
            if item['type'] == dataset_type and item['campaign'] == campaign:
                if 'LowPU' in item['processing_string']:
                    # Because in this table we _explicitly_ don't care about LowPU requests
                    continue

                if not processing_string or processing_string == item['processing_string']:
                    return item

    return {}


with open('data.json', 'r') as data_file:
    items = json.load(data_file)

print('Read %s items from data.json' % (len(items)))

exception_2016F_twiki = {'/BTagCSV/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/BTagMu/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/Charmonium/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/DisplacedJet/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/DoubleEG/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/DoubleMuon/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/DoubleMuonLowMass/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/JetHT/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/HTMHT/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/MET/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/MuOnia/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/MuonEG/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/SingleElectron/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/SingleMuon/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/SinglePhoton/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/Tau/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808],
                         '/ZeroBias/Run2016F-v1/RAW': [278769, 278801, 278802, 278803, 278804, 278805, 278808]}

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
        miniaod_v1 = pick_output_item(aod.get('output'), 'MiniAODv1')
        miniaod_v2 = pick_output_item(aod.get('output'), 'MiniAODv2')
        miniaod_v1_nanoaod_v8 = pick_output_item(miniaod_v1.get('output'), 'NanoAODv8')
        miniaod_v2_nanoaod_v9 = pick_output_item(miniaod_v2.get('output'), 'NanoAODv9')

        if aod:
            print('  AOD: %s (%s)' % (aod['dataset'], aod['type']))

        if miniaod_v1:
            print('  MiniAODv1: %s (%s)' % (miniaod_v1['dataset'], miniaod_v1['type']))

        if miniaod_v1_nanoaod_v8:
            print('    MiniAODv1 NanoAODv8: %s (%s)' % (miniaod_v1_nanoaod_v8['dataset'], miniaod_v1_nanoaod_v8['type']))

        if miniaod_v2:
            print('  MiniAODv2: %s (%s)' % (miniaod_v2['dataset'], miniaod_v2['type']))

        if miniaod_v2_nanoaod_v9:
            print('    MiniAODv2 NanoAOD: %s (%s)' % (miniaod_v2_nanoaod_v9['dataset'], miniaod_v2_nanoaod_v9['type']))

        twiki_runs = set(item['twiki_runs'])
        if '2016F' in raw_dataset:
            if 'HIPM' not in aod['processing_string']:
                twiki_runs = set(exception_2016F_twiki.get(raw_dataset, []))

        raw_runs = set(item.get('runs', []))
        raw_events = item.get('events', 0)
        raw_x_dcs_runs = set(aod.get('raw_x_dcs_runs', []))
        raw_x_dcs_events = aod.get('raw_x_dcs_events', 0)
        whitelist_runs = set(aod.get('whitelist_runs', []))
        whitelist_events = aod.get('whitelist_events', 0)
        whitelist_x_dcs_runs = set(aod.get('whitelist_x_dcs_runs', []))
        whitelist_x_dcs_events = aod.get('whitelist_x_dcs_events', 0)

        result = {'input_dataset': raw_dataset,
                  'year': item['year'],
                  'primary_dataset': raw_dataset.split('/')[1],
                  'twiki_runs': len(twiki_runs),

                  'raw_runs': len(raw_runs),
                  'raw_events': raw_events,
                  'raw_x_dcs_runs': len(raw_x_dcs_runs),
                  'raw_x_dcs_events': raw_x_dcs_events,
                  'whitelist_runs': len(whitelist_runs),
                  'whitelist_events': whitelist_events,
                  'whitelist_x_dcs_runs': len(whitelist_x_dcs_runs),
                  'whitelist_x_dcs_events': whitelist_x_dcs_events,

                  'twiki_and_whitelist_missing_runs': len(whitelist_runs - twiki_runs),
                  'twiki_and_whitelist_surplus_runs': len(twiki_runs - whitelist_runs),
                  'twiki_and_whitelist_runs_diff': len(whitelist_runs - twiki_runs) + len(twiki_runs - whitelist_runs),

                  'whitelist_and_raw_x_dcs_missing_runs': len(raw_x_dcs_runs - whitelist_runs),
                  'whitelist_and_raw_x_dcs_surplus_runs': len(whitelist_runs - raw_x_dcs_runs),
                  'whitelist_and_raw_x_dcs_runs': len(raw_x_dcs_runs - whitelist_runs) + len(whitelist_runs - raw_x_dcs_runs),
                  }

        for prefix, (thing, parent) in {'aod': (aod, None),
                                        'miniaod_v1': (miniaod_v1, aod),
                                        'miniaod_v1_nanoaod_v8': (miniaod_v1_nanoaod_v8, miniaod_v1),
                                        'miniaod_v2': (miniaod_v2, aod),
                                        'miniaod_v2_nanoaod_v9': (miniaod_v2_nanoaod_v9, miniaod_v2)}.items():
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
                result[prefix + '_produced_vs_parent_ratio'] = float(events) / whitelist_events if whitelist_events else None
                result[prefix + '_vs_parent_missing_runs'] = len(whitelist_runs - runs)
                result[prefix + '_vs_parent_surplus_runs'] = len(runs - whitelist_runs)
                result[prefix + '_vs_parent_runs_diff'] = result[prefix + '_vs_parent_missing_runs'] + result[prefix + '_vs_parent_surplus_runs']

        results.append(result)


with open('data_original_table.json', 'w') as output_file:
    json.dump(results, output_file, indent=1, sort_keys=True)

with open('original_table_timestamp.txt', 'w') as output_file:
    output_file.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
