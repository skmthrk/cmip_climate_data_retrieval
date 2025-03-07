import os
import pandas as pd

def variant_tuple(variant_label):
    rest = variant_label.split('r')[-1]
    r, rest = rest.split('i')
    i, rest = rest.split('p')
    p, f = rest.split('f')
    return (int(r), int(i), int(p), int(f))

data_dir = 'database_processed'
output_dir = 'queue_for_download'
os.makedirs(output_dir, exist_ok=True)

headers = ['source_id,activity_id,experiment_id,variant_label,variable,grid_label,filenum,filename,filesize,download_url,opendap_url']

def main():
    '''
    Identify the sources that satisfy:
     - datasets for piControl and abrupt-4xCO2 are available
     - these datasets include at least the following variables:
       - rsdt
       - rsut
       - rlut
       - tas
     - use the first variant label (usually r1i1p1f1)
     - identify a single dataset file for each (source_id, experiment_id, variable) tuple
       and generate a comma separated line including download urls and opendap urls for the file
     - calculate the total file size (in gigabyte)
    '''

    storage_required = 0
    storage_lines = []

    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            source_id, ext = os.path.splitext(filename)
            file_path = os.path.join(data_dir, filename)
            df = pd.read_csv(file_path)
            experiment_ids = sorted(list(set(df['experiment_id'])))
            lines = []
            filesize_total = 0
            if 'piControl' in experiment_ids and 'abrupt-4xCO2' in experiment_ids:
                for experiment_id in experiment_ids:
                    df_experiment = df[df['experiment_id'] == experiment_id]
                    variant_labels = list(set(df_experiment['variant_label']))
                    variant_labels.sort(key=lambda s: variant_tuple(s))
                    variant_label = variant_labels[0]
                    df_experiment_variant = df_experiment[df_experiment['variant_label'] == variant_label]
                    variables = set(df_experiment_variant['variable'])
                    if {'rsdt', 'rsut', 'rlut', 'tas'}.issubset(variables):
                        for variable in sorted(list(variables)):
                            df_experiment_variant_variable = df_experiment_variant[df_experiment_variant['variable'] == variable]
                            num_files = len(df_experiment_variant_variable)
                            for idx, (index, row) in enumerate(df_experiment_variant_variable.iterrows()):
                                source_id, activity_id, experiment_id, variant_label, variable, grid_label, filename, filesize, download_url, opendap_url = list(row)
                                filesize_total += int(filesize)
                                lines.append(f"{source_id},{activity_id},{experiment_id},{variant_label},{variable},{grid_label},{idx+1}/{num_files},{filename},{filesize},{download_url},{opendap_url}")
                if lines:
                    output = headers + lines
                    file_path_out = f"{output_dir}/{source_id}.csv"
                    with open(file_path_out, 'w') as f:
                        f.write('\n'.join(output))
                    print(f"Saved: {file_path_out}")
                    storage_required += filesize_total

                # file size variable in the data file is in byte
                # convert it into gigabytes (* 1.0e-9 # in gigabytes)
                # filesize * 8 # in bits
                # filesize * 1 # in bytes
                # filesize * 1.0e-3 # in kilobytes
                # filesize * 1.0e-6 # in megabytes
                # filesize * 1.0e-9 # in gigabytes
                storage_lines.append(f"{source_id},{float(filesize_total * 1.0e-9):.3f}GB")
    storage_lines.append(f"total,{float(storage_required * 1.0e-9):.3f}GB")
    with open(f"{output_dir}/storage_requirement.txt", 'w') as f:
        f.write('\n'.join(storage_lines))
    print(f"===> {float(storage_required * 1.0e-9):.3f} GB of storage required for downloading all files")

if __name__ == "__main__":
    main()
