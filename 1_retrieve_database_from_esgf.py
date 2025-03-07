import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyesgf.search import SearchConnection
from requests.exceptions import HTTPError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_file_metadata(dataset, data_node, retries=3, retry_delay=2):
    """
    Retrieves metadata for files associated with a dataset.

    Args:
        dataset: The dataset object from pyesgf.
        data_node: The data node associated with the dataset.
        retries: Number of times to retry on HTTP errors.
        retry_delay: Time in seconds to wait between retries.

    Returns:
        A list of strings, each containing file metadata, or None if retrieval fails after retries.
    """
    for attempt in range(retries):
        try:
            files = dataset.file_context().search()
            metadata_lines = []
            for f in files:
                metadata_lines.append(
                   ','.join(str(item) if item is not None else '' for item in
                           [dataset.dataset_id.split('|')[0],
                            data_node,
                            f.filename,
                            f.size,
                            f.download_url,
                            f.opendap_url]
                    )
                )
            return metadata_lines  # Success, return the list of metadata
        except HTTPError as e:
            logging.warning(f"HTTPError retrieving file metadata (attempt {attempt+1}/{retries}) for dataset {dataset.dataset_id}. Error: {e}")
            if attempt < retries - 1:
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to retrieve file metadata for dataset {dataset.dataset_id} after {retries} retries.")
                return None  # Failed after all retries


def search_cmip_data(experiment_id, variable, conn, retry_delay=2):
    '''
    Searches for CMIP data using ESGF API and returns file metadata.

     Args:
        experiment_id: The experiment ID to search for.
        variable: The variable to search for.
        conn: An ESGF SearchConnection object.
        retry_delay: Time in seconds to wait between retries when connecting to a new ESGF node.

    Returns:
        A list of strings, each a comma-separated line of file metadata, or an empty list if no suitable data is found.
    '''

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

    ctx = conn.new_context(**kwargs)
    try:
        results = ctx.search()
    except Exception as e:
        logging.error(f"Error during search for {experiment_id}.{variable}: {e}")
        return [] # Return empty list if search fails

    results = ctx.search()

    datasets = {}
    for result in results:
        master_id, data_node = result.dataset_id.split('|')
        project, activity_id, institution_id, source_id, experiment_id, variant_label, freq, variable, grid_label, *_ = master_id.split('.')
        if frequency == 'mon' and freq != 'Amon':
            continue  # Skip datasets with incorrect frequency
        datasets.setdefault(master_id, []).append((result, data_node))

    logging.info(f"{len(datasets)} datasets found for {experiment_id}.{variable}")

    lines = []
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
        future_to_dataset = {
            executor.submit(download_file_metadata, dataset, data_node): (dataset, data_node)
            for master_id, data_nodes in datasets.items()
                for dataset, data_node in data_nodes
            }

        for future in as_completed(future_to_dataset):
            dataset, data_node = future_to_dataset[future]
            try:
                metadata_lines = future.result()
                if metadata_lines:
                    lines.extend(metadata_lines)
            except Exception as e:
                logging.error(f"Error processing dataset {dataset.dataset_id}: {e}")
    return lines

#    master_ids = sorted(datasets.keys())
#    lines = []
#    for master_id in master_ids:
#        print(f"=== DATASET: {master_id}")
#        data_nodes = datasets[master_id]
#        for dataset, data_node in data_nodes:
#            success = True
#
#            # handle http 503 Error
#            retries = 3
#            for attempt in range(retries):
#                try:
#                    files = dataset.file_context().search()
#                    break  # Break out of the loop if successful
#                except HTTPError as e:
#                    if attempt < retries - 1:
#                        print(f"Retrying... ({attempt + 1})")
#                        time.sleep(retry_delay)  # Wait before retrying
#                    else:
#                        raise
#
#            for f in files:
#                file_id = f.file_id
#                filename = f.filename
#                download_url = f.download_url
#                opendap_url = f.opendap_url
#                size = str(f.size)
#                l = []
#                for itm in [master_id, data_node, filename, size, download_url, opendap_url]:
#                    if itm == None:
#                        itm = ''
#                    else:
#                        l.append(str(itm))
#                lines.append(','.join(l))
#    return lines


def main():

    # specify experiments and variables to download
    experiment_ids = ['piControl', 'abrupt-4xCO2', '1pctCO2', 'historical', 'ssp119', 'ssp245', 'ssp370', 'ssp460', 'ssp585', 'esm-piControl', 'esm-hist', 'esm-ssp585', 'esm-1pctCO2']
    variables = ["areacella", "tas", "rsdt", "rsut", "rlut"]

    # create output directory
    output_dir = "database"
    os.makedirs(output_dir, exist_ok=True)

    # specify the search domains in order
    search_domains = ["esgf-node.llnl.gov", "esgf-data.dkrz.de", "esgf-index1.ceda.ac.uk"]

    # generate database of cmip data
    headers = ['master_id,data_node,filename,size,download_url,opendap_url']
    for experiment_id in experiment_ids:
        for variable in variables:
            lines = []  # initialize empty lines for each (experiment_id, variable) combination
            for search_domain in search_domains:
                try:
                    conn = SearchConnection(f"https://{search_domain}/esg-search", distrib=True)
                    lines = search_cmip_data(experiment_id, variable, conn)
                    if lines:
                        break # Exit loop of search_domains if data is found in this node
                except Exception as e:
                    logging.error(f"Error connecting to {search_domain}: {e}")
            output = headers + lines
            with open(f"{output_dir}/{experiment_id}.{variable}.csv", 'w') as f:
                f.write('\n'.join(output))

if __name__ == "__main__":

    main()
