import os
from collections import defaultdict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

data_dir = 'database'
experiment_ids = ['piControl', 'abrupt-4xCO2', '1pctCO2', 'historical', 'ssp119', 'ssp245', 'ssp370', 'ssp460', 'ssp585', 'esm-piControl', 'esm-hist', 'esm-ssp585', 'esm-1pctCO2']
variables = ["areacella", "tas", "rsdt", "rsut", "rlut"]

output_dir = 'database_processed'
headers = ['soruce_id,activity_id,experiment_id,variant_label,variable,grid_label,filename,filesize,download_url,opendap_url']

def variant_tuple(variant_label):
    rest = variant_label.split('r')[-1]
    r, rest = rest.split('i')
    i, rest = rest.split('p')
    p, f = rest.split('f')
    return (int(r), int(i), int(p), int(f))

def variant_string(variant):
    return f"r{variant[0]}i{variant[1]}p{variant[2]}f{variant[3]}"

def process_csv_file(file_path, source_ids, dct_source, dct_filesize):
    with open(file_path, 'r') as f:
        next(f)
        for line in f:
            lst = line.strip().split(',')
            if len(lst) == 6:
                master_id, data_node, filename, size, download_url, opendap_url = lst
            else:
                master_id, data_node, filename, size, download_url = lst
                opendap_url = ''
            project, activity_id, institution_id, source_id, experiment_id, variant_label, frequency, variable, grid_label, *_ = master_id.split('.')
            if source_id not in source_ids:
                source_ids.append(source_id)
            dct_filesize[filename] = size
            dct_activity = dct_source.setdefault(source_id, {})
            dct_experiment = dct_activity.setdefault(activity_id, {})
            dct_variant = dct_experiment.setdefault(experiment_id, {})
            dct_variable = dct_variant.setdefault(variant_label, {})
            dct_grid = dct_variable.setdefault(variable, {})
            dct_file = dct_grid.setdefault(grid_label, {})
            dct_url = dct_file.setdefault(filename, {})
            dct_url.setdefault('download', []).append(download_url)
            dct_url.setdefault('opendap', []).append(opendap_url)

def main():
    '''
    Use cmip_database (csv files containing metadata of datasets, each csv file is for (experiment_id, variable) pair) generated in the previous step
    Reorganize the database and generate a csv file for each source
    Download urls for the same dataset file is combined with '|' separator
    '''

    source_ids = []
    dct_source = {}
    dct_filesize = {}
    for experiment_id in experiment_ids:
        for variable in variables:
            file_path = f"{data_dir}/{experiment_id}.{variable}.csv"
            process_csv_file(file_path, source_ids, dct_source, dct_filesize)

            # add extra data sources if exist
            extra_dir = f"{data_dir}/extra/{experiment_id}.{variable}"
            if os.path.isdir(extra_dir):
                csv_filenames = [filename for filename in os.listdir(extra_dir) if (not filename.startswith('.')) and filename.endswith('.csv')]
                for filename in csv_filenames:
                    file_path = os.path.join(extra_dir, filename)
                    process_csv_file(file_path, source_ids, dct_source, dct_filesize)

    # generate output
    os.makedirs(output_dir, exist_ok=True)
    for source_id in source_ids:
        lines = []
        for activity_id in dct_source[source_id].keys():
            for experiment_id in dct_source[source_id][activity_id].keys():
                variant_tuples = []
                for variant_label in dct_source[source_id][activity_id][experiment_id].keys():
                    variant_tuples.append(variant_tuple(variant_label))
                variant_tuples.sort()
                variant_labels = [variant_string(t) for t in variant_tuples]
                for variant_label in variant_labels:
                    for variable in dct_source[source_id][activity_id][experiment_id][variant_label].keys():
                        for grid_label in dct_source[source_id][activity_id][experiment_id][variant_label][variable]:
                            for filename in dct_source[source_id][activity_id][experiment_id][variant_label][variable][grid_label].keys():
                                download_urls = dct_source[source_id][activity_id][experiment_id][variant_label][variable][grid_label][filename]['download']
                                opendap_urls = dct_source[source_id][activity_id][experiment_id][variant_label][variable][grid_label][filename]['opendap']
                                filesize = dct_filesize[filename]
                                line = f"{source_id},{activity_id},{experiment_id},{variant_label},{variable},{grid_label},{filename},{filesize},{'|'.join(download_urls)},{'|'.join(opendap_urls)}"
                                lines.append(line)
        output = headers + lines

        file_path_out = f"{output_dir}/{source_id}.csv"
        with open(file_path_out, 'w') as f:
            f.write('\n'.join(output))
        print(f"Saved: {file_path_out}")

if __name__ == "__main__":
    main()
