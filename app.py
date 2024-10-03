import os
import xarray as xr
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, send_file, flash, render_template

app = Flask(__name__)

# Define the location where NetCDF files are stored
NETCDF_DIR = '/persistent_data/rainfall_nc/'
OUTPUT_DIR = '/persistent_data/output/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Maximum number of threads for parallel processing
MAX_WORKERS = 8

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

# Function to process a single coordinate for a single year
def process_single_coordinate_single_year(latitude, longitude, year):
    data_frames = []
    process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, year)

# Function to process a single coordinate for multiple years
def process_single_coordinate_multiple_years(latitude, longitude, start_year, end_year):
    data_frames = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_nc_file, year, latitude, longitude, data_frames) for year in range(start_year, end_year + 1)]
        for future in as_completed(futures):
            future.result()
    return prepare_and_send_csv(data_frames, latitude, longitude, f'{start_year}_{end_year}')

# Function to process multiple coordinates for a single year
def process_multiple_coordinates_single_year(excel_data, year):
    data_frames = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_nc_file, year, row['Latitude'], row['Longitude'], data_frames) for _, row in excel_data.iterrows()]
        for future in as_completed(futures):
            future.result()
    return prepare_and_send_csv(data_frames, 'multiple', 'multiple', year)

# Function to process multiple coordinates for multiple years
def process_multiple_coordinates_multiple_years(excel_data, start_year, end_year):
    data_frames = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_nc_file, year, row['Latitude'], row['Longitude'], data_frames)
            for year in range(start_year, end_year + 1)
            for _, row in excel_data.iterrows()
        ]
        for future in as_completed(futures):
            future.result()
    return prepare_and_send_csv(data_frames, 'multiple', 'multiple', f'{start_year}_{end_year}')

# Function to process a NetCDF file for the given year and coordinates
def process_nc_file(year, latitude, longitude, data_frames):
    file_path = os.path.join(NETCDF_DIR, f'RF25_ind{year}_rfp25.nc')
    df = extract_rainfall_data(file_path, latitude, longitude, year)
    if df is not None:
        data_frames.append((df, f"{year}_{latitude}_{longitude}"))

# Function to extract rainfall data from NetCDF file using xarray
def extract_rainfall_data(file_path, latitude, longitude, year):
    try:
        if os.path.exists(file_path):
            dataset = xr.open_dataset(file_path)  # Lazy load NetCDF with xarray
            latitudes = dataset['LATITUDE'].values
            longitudes = dataset['LONGITUDE'].values
            rainfall = dataset['RAINFALL'].sel(LATITUDE=latitude, LONGITUDE=longitude, method='nearest').values
            times = pd.to_datetime(dataset['TIME'].values, unit='D', origin=pd.Timestamp('1900-01-01'))

            df = pd.DataFrame({
                'Date': times,
                'Latitude': latitude,
                'Longitude': longitude,
                'Rainfall': rainfall
            })
            return df
    except Exception as e:
        print(f"Error processing NetCDF: {e}")
    return None

# Function to prepare and send CSV files
def prepare_and_send_csv(data_frames, latitude, longitude, year):
    output_file = os.path.join(OUTPUT_DIR, f'rainfall_data_{latitude}_{longitude}_{year}.csv')
    with open(output_file, mode='w', newline='', encoding='utf-8') as f:
        for df, sheet_name in data_frames:
            df.to_csv(f, index=False, header=True, mode='a')  # Append all data to a single CSV file
    return send_file(output_file, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
