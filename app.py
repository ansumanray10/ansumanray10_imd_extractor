import os
import netCDF4 as nc
import numpy as np
import pandas as pd
from flask import Flask, request, send_file, flash, render_template
import zipfile
from temp_extractor import process_grd_file, zip_files  # Import temp_extractor functions

app = Flask(__name__)

# Define directories for rainfall and temperature data
NETCDF_DIR = '/persistent_data/rainfall_nc/'  # For rainfall data
GRD_DIR = '/persistent_data/temp_grd/'  # For temperature data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/privacy_policy.html')
def privacy_policy():
    return render_template('privacy_policy.html')

@app.route('/terms_of_service.html')
def terms_of_service():
    return render_template('terms_of_service.html')

@app.route('/submit', methods=['POST'])
def submit():
    data_type = request.form.get('dataType')  # Get selected data type (Rainfall or Temperature)
    year_type = request.form.get('yearType')
    coord_type = request.form.get('coordType')

    try:
        if coord_type == 'single':
            latitude = request.form.get('latitude')
            longitude = request.form.get('longitude')

            if year_type == 'single':
                year = request.form.get('year')
                if year and latitude and longitude:
                    return process_single_coordinate_single_year(data_type, float(latitude), float(longitude), int(year))
                else:
                    flash("Please provide valid inputs for year, latitude, and longitude.")
                    return render_template('index.html')

            else:  # Range of years
                start_year = request.form.get('start_year')
                end_year = request.form.get('end_year')
                if start_year and end_year and latitude and longitude:
                    return process_single_coordinate_multiple_years(data_type, float(latitude), float(longitude), int(start_year), int(end_year))
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
                        return process_multiple_coordinates_single_year(data_type, excel_data, int(year))
                    else:
                        flash("Please provide a valid year.")
                        return render_template('index.html')

                else:  # Range of years
                    start_year = request.form.get('start_year')
                    end_year = request.form.get('end_year')
                    if start_year and end_year:
                        return process_multiple_coordinates_multiple_years(data_type, excel_data, int(start_year), int(end_year))
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

# Functions for processing temperature or rainfall data

def process_single_coordinate_single_year(data_type, latitude, longitude, year):
    """Process a single coordinate for a single year."""
    if data_type == 'rainfall':
        # Rainfall processing logic
        return process_rainfall_single_coordinate_single_year(latitude, longitude, year)
    else:
        # Temperature processing logic
        return process_temperature_single_coordinate_single_year(latitude, longitude, year)

def process_single_coordinate_multiple_years(data_type, latitude, longitude, start_year, end_year):
    """Process a single coordinate for multiple years."""
    if data_type == 'rainfall':
        return process_rainfall_single_coordinate_multiple_years(latitude, longitude, start_year, end_year)
    else:
        return process_temperature_single_coordinate_multiple_years(latitude, longitude, start_year, end_year)

def process_multiple_coordinates_single_year(data_type, excel_data, year):
    """Process multiple coordinates for a single year."""
    if data_type == 'rainfall':
        return process_rainfall_multiple_coordinates_single_year(excel_data, year)
    else:
        return process_temperature_multiple_coordinates_single_year(excel_data, year)

def process_multiple_coordinates_multiple_years(data_type, excel_data, start_year, end_year):
    """Process multiple coordinates for a range of years."""
    if data_type == 'rainfall':
        return process_rainfall_multiple_coordinates_multiple_years(excel_data, start_year, end_year)
    else:
        return process_temperature_multiple_coordinates_multiple_years(excel_data, start_year, end_year)

# Functions to process rainfall (already existing functions in your original code)

def process_rainfall_single_coordinate_single_year(latitude, longitude, year):
    """Processes a single coordinate for a single year of rainfall data and returns CSV."""
    data_frames = []
    process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, year)

def process_rainfall_single_coordinate_multiple_years(latitude, longitude, start_year, end_year):
    """Processes a single coordinate for a range of years of rainfall data and returns a zip of CSVs."""
    data_frames = []
    for year in range(start_year, end_year + 1):
        process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, f'{start_year}_{end_year}', is_multiple_files=True)

# Call the temperature extractor for temperature data
def process_temperature_single_coordinate_single_year(latitude, longitude, year):
    """Processes a single coordinate for a single year of temperature data and returns CSV."""
    grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
    output_folder = '/persistent_data/output'
    csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
    return send_file(csv_file, as_attachment=True)

def process_temperature_single_coordinate_multiple_years(latitude, longitude, start_year, end_year):
    """Processes a single coordinate for multiple years of temperature data and returns a zip of CSVs."""
    output_folder = '/persistent_data/output'
    csv_files = []
    for year in range(start_year, end_year + 1):
        grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
        csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
        csv_files.append(csv_file)
    zip_path = zip_files(csv_files, f'temperature_data_{latitude}_{longitude}_{start_year}_{end_year}.zip')
    return send_file(zip_path, as_attachment=True)

def process_temperature_multiple_coordinates_single_year(excel_data, year):
    """Processes multiple coordinates for a single year of temperature data and returns a zip of CSVs."""
    output_folder = '/persistent_data/output'
    csv_files = []
    for _, row in excel_data.iterrows():
        latitude = row['Latitude']
        longitude = row['Longitude']
        grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
        csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
        csv_files.append(csv_file)
    zip_path = zip_files(csv_files, f'temperature_data_multiple_coordinates_{year}.zip')
    return send_file(zip_path, as_attachment=True)

def process_temperature_multiple_coordinates_multiple_years(excel_data, start_year, end_year):
    """Processes multiple coordinates for multiple years of temperature data and returns a zip of CSVs."""
    output_folder = '/persistent_data/output'
    csv_files = []
    for year in range(start_year, end_year + 1):
        for _, row in excel_data.iterrows():
            latitude = row['Latitude']
            longitude = row['Longitude']
            grd_file = os.path.join(GRD_DIR, f'Maxtemp_MaxT_{year}.GRD')
            csv_file = process_grd_file(grd_file, latitude, longitude, output_folder, year)
            csv_files.append(csv_file)
    zip_path = zip_files(csv_files, f'temperature_data_multiple_coordinates_{start_year}_{end_year}.zip')
    return send_file(zip_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
