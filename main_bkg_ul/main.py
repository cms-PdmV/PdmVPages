import sys
import time
import random
sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
from rest import McM
import json


mcm = McM(dev=False)

mcm_cache = {}
def mcm_get(database, prepid):
    """
    Look for an object with given prepid
    If it is in cache, return cached object
    If not, fetch, save to cache and return it
    """
    if prepid in mcm_cache:
        return mcm_cache[prepid]

    result = mcm.get(database, prepid)
    mcm_cache[prepid] = result
    return result


def make_row(ds, root, mini, nano):
    """
    Make a single output row with given dataset, root, mini and nano requests
    """
    return {'root_prepid': root['prepid'] if root else '',
            'dataset': root['dataset_name'] if root else ds,
            'status': root['status'] if root else 'not_exist',
            'mini': mini['prepid'] if mini else '',
            'mini_status': mini['status'] if mini else '',
            'mini_total_events': mini['total_events'] if mini else 0,
            'mini_completed_events': mini['completed_events'] if mini else 0,
            'nano': nano['prepid'] if nano else '',
            'nano_status': nano['status'] if nano else '',
            'nano_total_events': nano['total_events'] if nano else 0,
            'nano_completed_events': nano['completed_events'] if nano else 0,
           }


with open('datasets.txt') as ds_file:
    datasets = sorted(list(set([d.strip() for d in ds_file.read().split('\n') if d.strip()])))


print('Read %s datasets from file' % (len(datasets)))
if '--debug' in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:10]
    print('Picking random %s datasets because debug' % (len(datasets)))


rows = []
for ds_i, ds_name in enumerate(datasets):
    requests = mcm.get('requests', query='prepid=*20UL*GEN*&dataset_name=%s' % (ds_name))
    print('%s/%s dataset %s fetched %s requests' % (ds_i + 1,
                                                    len(datasets),
                                                    ds_name,
                                                    len(requests)))
    if requests:
        for req_i, request in enumerate(requests):
            if 'PPD' in request['prepid']:
                continue
            print('  %s/%s request %s' % (req_i + 1, len(requests), request['prepid']))
            chain_ids = request['member_of_chain']
            if not chain_ids:
                rows.append(make_row(ds_name, request, None, None))
                continue
            for chain_i, chain_id in enumerate(chain_ids):
                print('    %s/%s chained request %s' % (chain_i + 1, len(chain_ids), chain_id))
                # condition to avoid JME Nano chains
                if 'NanoAODJME' in chain_id or 'NanoAODAPVJME' in chain_id: 
                    continue
                # condition to take chains up to nano
                if 'NanoAOD' not in chain_id:
                    continue
                # when you know the exactly thing you wanna fetch, instead of query
                chained_request = mcm_get('chained_requests', chain_id) 
                mini = None
                nano = None
                for req_family in chained_request['chain']:
                    if 'MiniAOD' in req_family:
                        mini = mcm_get('requests', req_family)
                    elif 'NanoAOD' in req_family:
                        nano = mcm_get('requests', req_family)

                rows.append(make_row(ds_name, request, mini, nano))
    else:
        # Fake requests to create rows in the table:
        rows.append(make_row(ds_name, None, None, None))


with open('data.json', 'w') as output_file:
    json.dump(rows, output_file, indent=2, sort_keys=True)


with open('update_timestamp.txt', 'w') as output_file:
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    output_file.write(date)

