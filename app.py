import os
import netCDF4 as nc
import numpy as np
import pandas as pd
from flask import Flask, request, send_file, flash, render_template

app = Flask(__name__)

# Define the location where NetCDF files are stored
NETCDF_DIR = '/persistent_data/rainfall_nc/'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit():
    year_type = request.form.get('yearType')
    coord_type = request.form.get('coordType')

    try:
        if coord_type == 'single':
            latitude = request.form.get('latitude')
            longitude = request.form.get('longitude')

            if year_type == 'single':
                year = request.form.get('year')
                if year and latitude and longitude:
                    return process_single_coordinate_single_year(float(latitude), float(longitude), int(year))
                else:
                    flash("Please provide valid inputs for year, latitude, and longitude.")
                    return render_template('index.html')

            else:  # Range of years
                start_year = request.form.get('start_year')
                end_year = request.form.get('end_year')
                if start_year and end_year and latitude and longitude:
                    return process_single_coordinate_multiple_years(float(latitude), float(longitude), int(start_year), int(end_year))
                else:
                    flash("Please provide valid inputs for start year, end year, latitude, and longitude.")
                    return render_template('index.html')

        elif coord_type == 'excel':
            coordinate_file = request.files.get('coordinateFile')
            if coordinate_file:
                excel_data = pd.read_excel(coordinate_file)
                if 'Latitude' not in excel_data.columns or 'Longitude' not in excel_data.columns:
                    flash("Excel file must contain 'Latitude' and 'Longitude' columns.")
                    return render_template('index.html')

                if year_type == 'single':
                    year = request.form.get('year')
                    if year:
                        return process_multiple_coordinates_single_year(excel_data, int(year))
                    else:
                        flash("Please provide a valid year.")
                        return render_template('index.html')

                else:  # Range of years
                    start_year = request.form.get('start_year')
                    end_year = request.form.get('end_year')
                    if start_year and end_year:
                        return process_multiple_coordinates_multiple_years(excel_data, int(start_year), int(end_year))
                    else:
                        flash("Please provide valid inputs for start year and end year.")
                        return render_template('index.html')
            else:
                flash("Please upload a valid Excel file.")
                return render_template('index.html')

    except Exception as e:
        print(f"An error occurred: {e}")
        flash(f"An error occurred: {e}")
        return render_template('index.html')

def process_single_coordinate_single_year(latitude, longitude, year):
    """Process a single coordinate for a single year and return the CSV file with one sheet."""
    data_frames = []
    process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, year)

def process_single_coordinate_multiple_years(latitude, longitude, start_year, end_year):
    """Process a single coordinate for a range of years and return the CSV file with multiple sheets."""
    data_frames = []
    for year in range(start_year, end_year + 1):
        process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, f'{start_year}_{end_year}', is_multiple_files=True)

def process_multiple_coordinates_single_year(excel_data, year):
    """Process multiple coordinates for a single year and return the CSV file with multiple sheets."""
    data_frames = []
    for _, row in excel_data.iterrows():
        process_nc_file(year, row['Latitude'], row['Longitude'], data_frames)
    return prepare_and_send_csv(data_frames, 'multiple', 'multiple', year, is_multiple_files=True)

def process_multiple_coordinates_multiple_years(excel_data, start_year, end_year):
    """Process multiple coordinates for a range of years and return the CSV file with multiple sheets."""
    data_frames = []
    for year in range(start_year, end_year + 1):
        for _, row in excel_data.iterrows():
            process_nc_file(year, row['Latitude'], row['Longitude'], data_frames)
    return prepare_and_send_csv(data_frames, 'multiple', 'multiple', f'{start_year}_{end_year}', is_multiple_files=True)

def process_nc_file(year, latitude, longitude, data_frames):
    """Processes the NetCDF file for the given year from the persistent storage."""
    try:
        # The path of the NetCDF file stored in persistent storage
        file_path = os.path.join(NETCDF_DIR, f'RF25_ind{year}_rfp25.nc')
        df = extract_rainfall_data(file_path, latitude, longitude, year)
        if isinstance(df, pd.DataFrame) and not df.empty:
            data_frames.append((df, f"{year}_{latitude}_{longitude}"))
            return True
        return False
    except Exception as e:
        print(f"Error processing file: {e}")
        return False

def extract_rainfall_data(file_path, target_lat, target_lon, year):
    """Extracts the rainfall data for the given coordinates from the NetCDF file."""
    try:
        if os.path.exists(file_path):
            dataset = nc.Dataset(file_path, mode='r')
            latitudes = dataset.variables['LATITUDE'][:]
            longitudes = dataset.variables['LONGITUDE'][:]
            rainfall = dataset.variables['RAINFALL'][:]
            times = dataset.variables['TIME'][:]
            time_units = dataset.variables['TIME'].units
            dates = nc.num2date(times, units=time_units)

            def find_nearest(array, value):
                return (np.abs(array - value)).argmin()

            lat_idx = find_nearest(latitudes, target_lat)
            lon_idx = find_nearest(longitudes, target_lon)
            rainfall_data = rainfall[:, lat_idx, lon_idx]

            df_extracted = pd.DataFrame({
                'Date': dates,
                'Latitude': [target_lat] * len(rainfall_data),
                'Longitude': [target_lon] * len(rainfall_data),
                'Rainfall': rainfall_data
            })

            dataset.close()
            return df_extracted

    except Exception as e:
        print(f"Error extracting data from NetCDF: {e}")
        return None

def prepare_and_send_csv(data_frames, latitude, longitude, year, is_multiple_files=False):
    """Prepares the response by concatenating data and sending the output as a CSV file."""
    output_folder = '/persistent_data/output'
    os.makedirs(output_folder, exist_ok=True)
    
    # Save the data as CSV
    if is_multiple_files:
        zip_path = os.path.join(output_folder, f'rainfall_data_{latitude}_{longitude}_{year}.zip')
        csv_paths = []
        for df, file_name in data_frames:
            csv_file = os.path.join(output_folder, f'{file_name}.csv')
            df.to_csv(csv_file, index=False)
            csv_paths.append(csv_file)
        
        # Create a zip file
        shutil.make_archive(zip_path.replace('.zip', ''), 'zip', output_folder)
        return send_file(zip_path, as_attachment=True)

    else:
        csv_file = os.path.join(output_folder, f'rainfall_data_{latitude}_{longitude}_{year}.csv')
        for df, _ in data_frames:
            df.to_csv(csv_file, index=False)
        return send_file(csv_file, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
