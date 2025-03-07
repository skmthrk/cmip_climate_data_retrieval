import os

import xarray as xr
import numpy as np

from utils import area, make_logger

logger = make_logger()

def build_data(input_dir, output_dir, file_names):
    """
    Build processed data from raw data files.

    Args:
        input_dir (str): Input directory containing raw data files.
        output_dir (str): Output directory to save processed data files.
        file_names (lst): List of netCDF file names for a particular (source_id, experiment_id, variable) in input_dir

    Returns:
        None
    """

    output_data = []
    for file_name in file_names:
        logger.info(f"Processing {file_name}")

        file_path = os.path.join(input_dir, file_name)
        ds = xr.open_dataset(file_path)
        variable_id = ds.variable_id
        da = ds[variable_id]

        # for output file name
        var_id, _, model_id, experiment_id, variant_id, grid_type, duration = file_name.split('_')

        try:
            # load areacella file
            area_file_name = f"areacella_fx_{model_id}_{experiment_id}_{variant_id}_{grid_type}.nc"
            area_file_path = os.path.join(input_dir, area_file_name)
            area_ds = xr.open_dataset(area_file_path)
            area_da = area_ds[area_ds.variable_id]
            area_da.data = area_da.data * 1e-6 # convert m2 to km2
            area_da.attrs["units"] = "km2"
        except FileNotFoundError:
            logger.warning(f"areacella file not found: {area_file_name}. Generating area data array.")
            area_da = area(da)

        # compute annual mean
        years = []
        annual_values = []

        # for month weight
        numdays_of_month = {1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}

        time = da['time']
        current_year = int(time[0].dt.year.values)
        month_values = []
        for idx_t, t in enumerate(time):
            year = int(t.dt.year.values)
            month = int(t.dt.month.values)

            # spatial aggregation
            month_value = np.nansum(da.sel(time=t).data * area_da.data/area_da.values.sum())
            month_weight = numdays_of_month[month]/365
            month_value *= month_weight

            if year == current_year:
                # keep adding month_value until year switch
                month_values.append(month_value)
            else:
                # switch to next year
                if len(month_values) == 12:
                    annual_value = sum(month_values)
                    annual_values.append(annual_value)
                    years.append(current_year)
                month_values = [month_value]
                current_year = year

            # last index
            if idx_t == len(time)-1:
                # save only if values exist for full year
                if len(month_values) == 12:
                    annual_values.append(sum(month_values))
                    years.append(year)

        # generate output file
        lines = [f"{year},{annual_value}" for year, annual_value in zip(years, annual_values)]
        output_data.append((years[0], lines))

    # save
    output_data.sort()
    output = []
    for _, lines in output_data:
        output += lines
    if output:
        file_name = f"{var_id}_{model_id}_{experiment_id}.csv"
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, 'w') as f:
            f.write(f"year,{var_id}\n")
            f.write('\n'.join(output))

def main():

    database_dir = './queue_for_download'
    input_dir = './downloaded'
    output_dir = './data_aggregated'

    source_ids = {}
    for fname in os.listdir(database_dir):
        if not fname.endswith('.csv'):
            continue
        source_id, _ = os.path.splitext(fname)
        experiments = {}
        with open(os.path.join(database_dir, fname), 'r') as f:
            next(f)
            for line in f:
                lst = [itm.strip() for itm in line.split(',')]
                source_id, activity_id, experiment_id, variant_label, variable, grid_label, filenum, filename, filesize, download_url, opendap_url = lst
                if variable == 'areacella':
                    continue
                variables = experiments.setdefault(experiment_id, {})
                variables.setdefault(variable, []).append(filename)
        source_ids[source_id] = experiments

    os.makedirs(output_dir, exist_ok=True)

    for source_id in source_ids:
        experiments = source_ids[source_id]
        for experiment in experiments:
            variables = experiments[experiment]
            for variable in variables:
                output_file_path = os.path.join(output_dir, f"{variable}_{source_id}_{experiment}.csv")
                if os.path.exists(output_file_path):
                    #print(f"Already exists: {output_file_path}")
                    continue
                file_names = variables[variable]
                try:
                    build_data(input_dir, output_dir, file_names)
                except Exception as e:
                    logger.warning(f"Error in processing {file_names}: {e}")

if __name__ == '__main__':
    main()
