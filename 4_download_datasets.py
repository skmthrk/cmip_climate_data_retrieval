import sys, os
import time

import requests
from requests.exceptions import HTTPError

import xarray as xr

data_dir = 'queue_for_download'
output_dir = 'downloaded'
os.makedirs(output_dir, exist_ok=True)

def download(filename, download_url, output_dir=output_dir):
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

def opendap(filename, opendap_url, output_dir=output_dir):
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

def main():

    failed_filenames = []
    for filename in os.listdir(data_dir):
        if filename.startswith('.'):
            continue
        elif filename.endswith('.csv'):
            source_id, ext = os.path.splitext(filename)
            print(f"--- {source_id}")
            with open(os.path.join(data_dir, filename), 'r') as f:
                next(f)
                lines = [line for line in f]
                num_lines = len(lines)
                for idx, line in enumerate(lines):
                    source_id, activity_id, experiment_id, variant_label, variable, grid_label, filenum, filename, filesize, download_urls, opendap_urls = line.split(',')
                    success = False
                    if os.path.isfile(os.path.join(output_dir, filename)):
                        #print(f'{idx+1:>3d}/{num_lines}: Found {filename}')
                        continue
                    print(f'{idx+1:>3d}/{num_lines}: Downloading {filename}')
                    for download_url in download_urls.split('|'):
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
                        for opendap_url in opendap_urls.split('|'):
                            try:
                                #print(f' - {opendap_url}')
                                opendap(filename, opendap_url)
                                success = True
                                print('===> Done!')
                                break
                            except Exception as e:
                                pass
                        print(f'===> Options exhausted!: {filename}')

    with open(os.path.join(output_dir, 'failed_download.txt'), 'w') as f:
        f.write('\n'.join(failed_filenames))

if __name__ == "__main__":
    main()
