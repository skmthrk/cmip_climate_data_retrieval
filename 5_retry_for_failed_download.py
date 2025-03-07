import sys, os
import time

from pyesgf.search import SearchConnection
import requests
from requests.exceptions import HTTPError

import xarray as xr

data_dir = 'downloaded'
os.makedirs(data_dir, exist_ok=True)

def download(filename, download_url, output_dir=data_dir):
    retries = 3
    for attempt in range(retries):
        try:
            # http download
            response = requests.get(download_url, stream=True, timeout=30)
            response.raise_for_status()
            with open(os.path.join(output_dir, filename), 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            break  # Break out of the loop if successful
        except HTTPError as e:
            if attempt < retries - 1:
                #print(f"Retrying... ({attempt + 1})")
                time.sleep(2)  # Wait before retrying
            else:
                raise

def opendap(filename, opendap_url, output_dir=data_dir):
    retries = 3
    for attempt in range(retries):
        try:
            dataset = xr.open_dataset(opendap_url)
            dataset.to_netcdf(os.path.join(output_dir, filename))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)  # Wait before retrying
            else:
                raise

def search_cmip_data(experiment_id, variable, conn, source_id=None):

    facets = 'project,source_id,experiment_id,variable,frequency,latest'
    project = 'CMIP6'
    frequency = 'fx' if variable == 'areacella' else 'mon'

    kwargs = {
        'facets': facets,
        'project': project,
        #activity_id': 'CMIP', # CMIP, C4MIP, ScenarioMIP, ...
        'experiment_id': experiment_id,
        'variable': variable,
        'frequency': frequency,
        'latest': True,
        #'variant_label': 'r1i1p1f2',
    }

    if source_id is not None:
        kwargs['source_id'] = source_id

    ctx = conn.new_context(**kwargs)
    results = ctx.search()

    datasets = {}
    for result in results:
        master_id, data_node = result.dataset_id.split('|')
        project, activity_id, institution_id, source_id, experiment_id, variant_label, freq, variable, grid_label, *_ = master_id.split('.')
        if frequency == 'mon' and freq != 'Amon':
            continue
        datasets.setdefault(master_id, []).append((result, data_node))

    print(f"{len(datasets)} datasets found for {experiment_id}.{variable}")
    master_ids = sorted(datasets.keys())
    lines = []
    for master_id in master_ids:
        print(f"=== DATASET: {master_id}")
        data_nodes = datasets[master_id]
        for dataset, data_node in data_nodes:
            success = True

            # handle http 503 Error
            retries = 3
            for attempt in range(retries):
                try:
                    files = dataset.file_context().search()
                    break  # Break out of the loop if successful
                except HTTPError as e:
                    if attempt < retries - 1:
                        print(f"Retrying... ({attempt + 1})")
                        time.sleep(2)  # Wait before retrying
                    else:
                        raise

            for f in files:
                file_id = f.file_id
                filename = f.filename
                download_url = f.download_url
                opendap_url = f.opendap_url
                size = str(f.size)
                l = []
                for itm in [master_id, data_node, filename, size, download_url, opendap_url]:
                    if itm == None:
                        itm = ''
                    else:
                        l.append(str(itm))
                lines.append(','.join(l))
    return lines

target_files = []
with open(os.path.join(data_dir, 'failed_download.txt'), 'r') as f:
    for line in f:
        if line.strip().endswith('.nc'):
            filename = line.strip()
            # skip if the file already exists
            if os.path.isfile(os.path.join(data_dir, filename)):
                continue
            target_files.append(filename)

print('===> Try to find urls for:')
print('\n'.join(target_files))

search_domains = ["esgf-node.llnl.gov", "esgf-data.dkrz.de", "esgf-index1.ceda.ac.uk"]
output_base_dir = "database/extra"

headers = ['master_id,data_node,filename,size,download_url,opendap_url']
dct_download_urls = {}
dct_opendap_urls = {}
for target_file in target_files:
    variable, freq, source_id, experiment_id, variant_label, grid_label, *_ = target_file.split('_')

    attempt = 0
    for search_domain in search_domains:
        try:
            conn = SearchConnection(f"https://{search_domain}/esg-search", distrib=True)
            lines = search_cmip_data(experiment_id, variable, conn, source_id=source_id)
            break
        except Exception as e:
            print(f"Error in connecting to {search_domain}: {e}")
    for line in lines:
        master_id, data_node, filename, size, download_url, opendap_url = line.split(',')
        if filename == target_file:
            dct_download_urls.setdefault(target_file, []).append(download_url)
            dct_opendap_urls.setdefault(target_file, []).append(download_url)
    output = headers + lines
    out_dir = os.path.join(output_base_dir, f"{experiment_id}.{variable}")
    os.makedirs(out_dir, exist_ok=True)
    with open(f"{out_dir}/{experiment_id}.{variable}.{source_id}.csv", 'w') as f:
        f.write('\n'.join(output))

failed_filenames = []
for filename in target_files:
    print(f'===> Try downloading {filename}')
    download_urls = list(set(dct_download_urls.get(filename, [])))
    opendap_urls = list(set(dct_opendap_urls.get(filename, [])))
    if not download_urls:
        print('No url is found')
        continue
    print('URLS:')
    for download_url in download_urls:
        print(f' - {download_url}')
    success = False
    for download_url in download_urls:
        try:
            download(filename, download_url)
            success = True
            break
        except Exception as e:
            #print(f"Error downloading {filename} from {download_url}: {e}")
            pass
    if not success:
        failed_filenames.append(filename)
        print(f"===> Failed to download {filename}")
        print(f"===> Trying opendap download")
        for opendap_url in opendap_urls:
            try:
                #print(f' - {opendap_url}')
                opendap(filename, opendap_url)
                success = True
                print('===> Done!')
                break
            except Exception as e:
                pass
        print(f'===> Options exhausted!: {filename}')

with open(os.path.join(data_dir, 'still_failed_download.txt'), 'w') as f:
    f.write('\n'.join(failed_filenames))
