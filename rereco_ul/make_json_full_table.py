import json
import time


def get_output_rows(item, path, rows):
    path = list(path)
    path.append(item)
    if not item.get('output'):
        item.pop('output', None)
        rows.append(path)
    else:
        for i in item.pop('output'):
            get_output_rows(i, path, rows)


def process_item(item):
    for attr in item.keys():
        if isinstance(item.get(attr), list) and (attr == 'runs' or attr.endswith('_runs')):
            item[attr] = len(item[attr])

    for output_item in item.get('output', []):
        process_item(output_item)


def calculate_fractions(item, parent):
    if 'whitelist_runs' in item:
        aod = item
        aod['whitelist_and_raw_x_dcs_surplus_runs'] = len(set(aod['whitelist_runs']) - set(aod['raw_x_dcs_runs']))
        aod['whitelist_and_raw_x_dcs_missing_runs'] = len(set(aod['raw_x_dcs_runs']) - set(aod['whitelist_runs']))
        aod['whitelist_and_raw_x_dcs_events_diff'] = aod['whitelist_events'] - aod['raw_x_dcs_events']

        aod['whitelist_x_dcs_and_raw_x_dcs_surplus_runs'] = len(set(aod['whitelist_x_dcs_runs']) - set(aod['raw_x_dcs_runs']))
        aod['whitelist_x_dcs_and_raw_x_dcs_missing_runs'] = len(set(aod['raw_x_dcs_runs']) - set(aod['whitelist_x_dcs_runs']))
        aod['whitelist_x_dcs_and_raw_x_dcs_events_diff'] = aod['whitelist_x_dcs_events'] - aod['raw_x_dcs_events']

        aod['fraction'] = aod['events'] / aod['raw_x_dcs_events'] if aod['raw_x_dcs_events'] else None
        aod['missing_runs'] = len(set(aod['raw_x_dcs_runs']) - set(aod['runs']))
        aod['surplus_runs'] = len(set(aod['runs']) - set(aod['raw_x_dcs_runs']))
        aod['events_difference'] = aod['events'] - aod['raw_x_dcs_events']

    elif parent:
        item['fraction'] = item['events'] / parent['events'] if parent['events'] else None
        item['missing_runs'] = len(set(parent['runs']) - set(item['runs']))
        item['surplus_runs'] = len(set(item['runs']) - set(parent['runs']))
        item['events_difference'] = item['events'] - parent['events']

    for output_item in item.get('output', []):
        calculate_fractions(output_item, item)


with open('data.json', 'r') as data_file:
    items = json.load(data_file)

print('Read %s items from data.json' % (len(items)))

results = []
for item in items:
    calculate_fractions(item, None)
    # Show only lengths of run lists
    process_item(item)
    # Rows of table
    output_rows = []
    get_output_rows(item, [], output_rows)
    results.extend(output_rows)

with open('data_full_table.json', 'w') as output_file:
    json.dump(results, output_file, indent=2, sort_keys=True)

with open('full_table_timestamp.txt', 'w') as output_file:
    output_file.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
