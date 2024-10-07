import os
import numpy as np
import pandas as pd
from flask import request, send_file, flash, render_template
import zipfile

# Define the location where GRD files are stored
GRD_DIR = '/persistent_data/temp_grd/'

def submit_temperature():
    year_type = request.form.get('yearType')
    coord_type = request.form.get('coordType')

    output_folder = '/persistent_data/output'
    os.makedirs(output_folder, exist_ok=True)

    try:
        if coord_type == 'single':
            latitude = float(request.form.get('latitude'))
            longitude = float(request.form.get('longitude'))

            if year_type == 'single':
                year = int(request.form.get('year'))
                return process_single_coordinate_single_year(latitude, longitude, year, output_folder)
            else:
                start_year = int(request.form.get('start_year'))
                end_year = int(request.form.get('end_year'))
                return process_single_coordinate_multiple_years(latitude, longitude, start_year, end_year, output_folder)

        elif coord_type == 'excel':
            coordinate_file = request.files.get('coordinateFile')
            if not coordinate_file:
                flash("Please upload an Excel file with coordinates.")
                return render_template('index.html')

            coordinates_df = pd.read_excel(coordinate_file)

            if 'Latitude' not in coordinates_df.columns or 'Longitude' not in coordinates_df.columns:
                flash("Excel file must contain 'Latitude' and 'Longitude' columns.")
                return render_template('index.html')

            if year_type == 'single':
                year = int(request.form.get('year'))
                return process_multiple_coordinates_single_year(coordinates_df, year, output_folder)
            else:
                start_year = int(request.form.get('start_year'))
                end_year = int(request.form.get('end_year'))
                return process_multiple_coordinates_multiple_years(coordinates_df, start_year, end_year, output_folder)

    except Exception as e:
        flash(f"An error occurred: {e}")
        return render_template('index.html')

# Function to process GRD file for a single year and single coordinate
def process_single_coordinate_single_year(latitude, longitude, year, output_folder):
    grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
    csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
    return send_file(csv_file, as_attachment=True)

# Function to process GRD files for a single coordinate over multiple years
def process_single_coordinate_multiple_years(latitude, longitude, start_year, end_year, output_folder):
    csv_files = []
    for year in range(start_year, end_year + 1):
        grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
        csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
        csv_files.append(csv_file)
    zip_path = zip_files(csv_files, f'temperature_data_{latitude}_{longitude}_{start_year}_{end_year}.zip')
    return send_file(zip_path, as_attachment=True)

# Function to process GRD files for multiple coordinates for a single year
def process_multiple_coordinates_single_year(coordinates_df, year, output_folder):
    csv_files = []
    for _, row in coordinates_df.iterrows():
        latitude = row['Latitude']
        longitude = row['Longitude']
        grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
        csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
        csv_files.append(csv_file)
    zip_path = zip_files(csv_files, f'temperature_data_multiple_coordinates_{year}.zip')
    return send_file(zip_path, as_attachment=True)

# Function to process GRD files for multiple coordinates over multiple years
def process_multiple_coordinates_multiple_years(coordinates_df, start_year, end_year, output_folder):
    csv_files = []
    for year in range(start_year, end_year + 1):
        for _, row in coordinates_df.iterrows():
            latitude = row['Latitude']
            longitude = row['Longitude']
            grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
            csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
            csv_files.append(csv_file)
    zip_path = zip_files(csv_files, f'temperature_data_multiple_coordinates_{start_year}_{end_year}.zip')
    return send_file(zip_path, as_attachment=True)

# Function to process the GRD file for a single coordinate
def process_grd_file(grd_file, latitude, longitude, output_folder, year, grid_shape=(31, 31), dtype=np.float32):
    # Define latitude and longitude grid points
    latitudes = np.linspace(7.5, 37.5, grid_shape[0])  # Latitudes from 7.5N to 37.5N
    longitudes = np.linspace(67.5, 97.5, grid_shape[1])  # Longitudes from 67.5E to 97.5E

    # Load the GRD data using numpy
    with open(grd_file, 'rb') as f:
        data = np.fromfile(f, dtype=dtype)

    # Calculate number of days by dividing total data points by the grid shape
    num_days = data.size // (grid_shape[0] * grid_shape[1])

    # Reshape the data into (days, latitudes, longitudes)
    data = data.reshape((num_days, grid_shape[0], grid_shape[1]))

    # Find the exact matching grid point for the given coordinates
    lat_idx = np.where(latitudes == latitude)[0][0]
    lon_idx = np.where(longitudes == longitude)[0][0]

    # Extract the temperature data for all days at the grid point
    temperature_values = data[:, lat_idx, lon_idx]

    # Handle no-data value (99.9 in your case); replace with NaN
    temperature_values = np.where(temperature_values == 99.9, np.nan, temperature_values)

    # Generate dates for the year (assuming 1 year of daily data)
    days = pd.date_range(start=f"{year}-01-01", periods=num_days, freq='D')

    # Create a DataFrame for the extracted data
    df = pd.DataFrame({
        'Date': days,
        'Latitude': latitude,
        'Longitude': longitude,
        'Temperature': temperature_values
    })

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Save the DataFrame to an Excel file
    output_file = os.path.join(output_folder, f"temperature_data_{latitude}_{longitude}_{year}.csv")
    df.to_csv(output_file, index=False)

    return output_file

# Function to zip multiple CSV files
def zip_files(files, zip_filename):
    zip_path = os.path.join('/persistent_data/output', zip_filename)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in files:
            zipf.write(file, os.path.basename(file))
    return zip_path
