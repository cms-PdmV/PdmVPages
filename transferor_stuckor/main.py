import json
import time
import os
from urllib.request import Request, urlopen
from connection_wrapper import ConnectionWrapper


cmsweb = ConnectionWrapper('cmsweb.cern.ch', keep_open=True)
try:
    # Try to import and setup rucio
    os.environ['RUCIO_CONFIG'] = 'rucio.cfg'
    from rucio.client.ruleclient import RuleClient
    rucio = RuleClient()
except Exception as ex:
    print('Error setting up rucio %s' % (ex))
    rucio = None


def make_simple_request(url):
    req = Request(url)
    try:
        return json.loads(urlopen(req).read().decode('utf-8'))
    except Exception as ex:
        print('Error while making a request to %s. Error %s' % (url, ex))
        print(ex)

    return None


def make_transferor_request(workflow_name):
    workflow = cmsweb.api('GET', '/ms-transferor/data/info?request=%s' % (workflow_name))
    workflow = json.loads(workflow).get('result')
    if workflow:
        return workflow[0]

    return None


def yield_staging_workflows():
    page = 0
    results = []
    fetched_results = [{}]
    page_size = 100
    while len(fetched_results) > 0:
        url = 'http://vocms074:5984/requests/_design/_designDoc/_view/lastStatus?key="staging"&limit=%s&skip=%s&include_docs=True' % (page_size, page_size * page)
        fetched_results = [x['doc'] for x in make_simple_request(url).get('rows', [])]
        results += fetched_results
        print('Fetched page %s. Found %s, total %s' % (page, len(fetched_results), len(results)))
        for result in fetched_results:
            yield result

        time.sleep(0.25)
        page += 1


def get_rucio_rules(transfers):
    rules = {'OK': [], 'REPLICATING': [], 'STUCK': [], 'SUSPENDED': []}
    if rucio:
        try:
            print('Getting rules for %s transfers' % (len(transfers)))
            for transfer_id in transfers:
                rule = rucio.get_replication_rule(transfer_id)
                state = rule.get('state').upper()
                rules.setdefault(state).append(transfer_id)
        except Exception as ex:
            print('Rucio error %s' % (ex))

    return rules


results = []
for workflow in yield_staging_workflows():
    workflow_name = workflow['RequestName']
    print('Name: %s' % (workflow_name))
    staging_timestamp = 0
    for entry in reversed(workflow['RequestTransition']):
        if entry['Status'] == 'staging':
            staging_timestamp = entry['UpdateTime']
            break

    print('  Staging timestamp: %s' % (staging_timestamp))
    transfer_doc = make_transferor_request(workflow_name).get('transferDoc')
    if not transfer_doc:
        continue

    transfers = transfer_doc.get('transfers', [])
    for transfer in transfers:
        completion = transfer.get('completion', [0.0])
        transfers = transfer.get('transferIDs', [])
        rucio_rules = get_rucio_rules(transfers)
        transfer_count = len(transfers)
        dataset = transfer.get('dataset')
        datatype = transfer.get('dataType', 'primary')
        last_completion = completion[-1]
        first_completion = completion[1]
        last_update = transfer_doc.get('lastUpdate')
        update_interval = step_time = (last_update - staging_timestamp) / (len(completion) - 1)
        time_in_staging = last_update - staging_timestamp
        print('  Completion %s' % (completion))
        print('  Last update %s' % (last_update))
        print('  Time staging %s (%s)' % (time_in_staging, staging_timestamp))
        print('  Update interval %s' % (update_interval))
        last_increase_index = 0
        for i in range(len(completion) - 1, 0, -1):
            if completion[i] != completion[i - 1]:
                last_increase_index = i
                break

        print('  Same completion index %s' % (last_increase_index))
        stuck_time = int(last_update - (staging_timestamp + last_increase_index * update_interval))
        print('  "Stuck" for %s' % (stuck_time))
        time_in_staging_without_first = last_update - staging_timestamp - update_interval
        speed = ((last_completion - first_completion) / time_in_staging_without_first) if time_in_staging_without_first else 0
        eta = ((100 - last_completion) / speed) if speed else 0
        speed = float('%.2f' % (speed * 86400))
        print('  Speed %.2f%%/day' % (speed))
        print('  ETA %ss' % (eta))
        results.append({'workflow': workflow_name,
                        'dataset': dataset,
                        'datatype': datatype,
                        'first_completion': first_completion,
                        'last_completion': last_completion,
                        'time_in_staging': time_in_staging,
                        'stuck_time': stuck_time,
                        'speed': speed,
                        'eta': eta,
                        'rucio_rules': rucio_rules,
                        'transfers': transfer_count})

with open('data.json', 'w') as output_file:
    json.dump(results, output_file, indent=1)


with open('update_timestamp.txt', 'w') as output_file:
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    output_file.write(date)
