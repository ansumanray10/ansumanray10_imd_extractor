import os
import netCDF4 as nc
import numpy as np
import pandas as pd
from flask import Flask, request, send_file, flash, render_template
import zipfile
from temp_extractor import extract_coordinates_data as extract_temp_data  # Import temp extraction function

app = Flask(__name__)

# Define the location where NetCDF files are stored
NETCDF_DIR = '/persistent_data/rainfall_nc/'  # For rainfall NetCDF files
TEMP_DIR = '/persistent_data/temp_grd/'  # For temperature .GRD files

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
    year_type = request.form.get('yearType')
    coord_type = request.form.get('coordType')
    data_type = request.form.get('dataType')  # Get whether user selects Rainfall or Temperature

    try:
        if coord_type == 'single':
            latitude = request.form.get('latitude')
            longitude = request.form.get('longitude')

            if year_type == 'single':
                year = request.form.get('year')
                if year and latitude and longitude:
                    if data_type == 'rainfall':
                        return process_single_coordinate_single_year_rainfall(float(latitude), float(longitude), int(year))
                    elif data_type == 'temperature':
                        return process_single_coordinate_single_year_temperature(float(latitude), float(longitude), int(year))
                else:
                    flash("Please provide valid inputs for year, latitude, and longitude.")
                    return render_template('index.html')

            else:  # Range of years
                start_year = request.form.get('start_year')
                end_year = request.form.get('end_year')
                if start_year and end_year and latitude and longitude:
                    if data_type == 'rainfall':
                        return process_single_coordinate_multiple_years_rainfall(float(latitude), float(longitude), int(start_year), int(end_year))
                    elif data_type == 'temperature':
                        return process_single_coordinate_multiple_years_temperature(float(latitude), float(longitude), int(start_year), int(end_year))
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
                        if data_type == 'rainfall':
                            return process_multiple_coordinates_single_year_rainfall(excel_data, int(year))
                        elif data_type == 'temperature':
                            return process_multiple_coordinates_single_year_temperature(excel_data, int(year))
                    else:
                        flash("Please provide a valid year.")
                        return render_template('index.html')

                else:  # Range of years
                    start_year = request.form.get('start_year')
                    end_year = request.form.get('end_year')
                    if start_year and end_year:
                        if data_type == 'rainfall':
                            return process_multiple_coordinates_multiple_years_rainfall(excel_data, int(start_year), int(end_year))
                        elif data_type == 'temperature':
                            return process_multiple_coordinates_multiple_years_temperature(excel_data, int(start_year), int(end_year))
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

# Rainfall Processing Functions (unchanged)
def process_single_coordinate_single_year_rainfall(latitude, longitude, year):
    """Process a single coordinate for a single year for rainfall data."""
    data_frames = []
    process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, year)

def process_single_coordinate_multiple_years_rainfall(latitude, longitude, start_year, end_year):
    """Process a single coordinate for a range of years for rainfall data."""
    data_frames = []
    for year in range(start_year, end_year + 1):
        process_nc_file(year, latitude, longitude, data_frames)
    return prepare_and_send_csv(data_frames, latitude, longitude, f'{start_year}_{end_year}', is_multiple_files=True)

def process_multiple_coordinates_single_year_rainfall(excel_data, year):
    """Process multiple coordinates for a single year for rainfall data."""
    data_frames = []
    for _, row in excel_data.iterrows():
        process_nc_file(year, row['Latitude'], row['Longitude'], data_frames)
    return prepare_and_send_csv(data_frames, 'multiple', 'multiple', year, is_multiple_files=True)

def process_multiple_coordinates_multiple_years_rainfall(excel_data, start_year, end_year):
    """Process multiple coordinates for a range of years for rainfall data."""
    data_frames = []
    for year in range(start_year, end_year + 1):
        for _, row in excel_data.iterrows():
            process_nc_file(year, row['Latitude'], row['Longitude'], data_frames)
    return prepare_and_send_csv(data_frames, 'multiple', 'multiple', f'{start_year}_{end_year}', is_multiple_files=True)

# Temperature Processing Functions
def process_single_coordinate_single_year_temperature(latitude, longitude, year):
    """Process a single coordinate for a single year for temperature data."""
    output_folder = '/persistent_data/output/'
    grd_file = os.path.join(TEMP_DIR, f'Maxtemp_MaxT_{year}.GRD')
    extract_temp_data(grd_file, [{'Latitude': latitude, 'Longitude': longitude}], output_folder)
    zip_path = create_zip(output_folder)
    return send_file(zip_path, as_attachment=True)

def process_single_coordinate_multiple_years_temperature(latitude, longitude, start_year, end_year):
    """Process a single coordinate for a range of years for temperature data."""
    output_folder = '/persistent_data/output/'
    for year in range(start_year, end_year + 1):
        grd_file = os.path.join(TEMP_DIR, f'Maxtemp_MaxT_{year}.GRD')
        extract_temp_data(grd_file, [{'Latitude': latitude, 'Longitude': longitude}], output_folder)
    zip_path = create_zip(output_folder)
    return send_file(zip_path, as_attachment=True)

def process_multiple_coordinates_single_year_temperature(excel_data, year):
    """Process multiple coordinates for a single year for temperature data."""
    output_folder = '/persistent_data/output/'
    grd_file = os.path.join(TEMP_DIR, f'Maxtemp_MaxT_{year}.GRD')
    coordinates = excel_data.to_dict('records')
    extract_temp_data(grd_file, coordinates, output_folder)
    zip_path = create_zip(output_folder)
    return send_file(zip_path, as_attachment=True)

def process_multiple_coordinates_multiple_years_temperature(excel_data, start_year, end_year):
    """Process multiple coordinates for a range of years for temperature data."""
    output_folder = '/persistent_data/output/'
    coordinates = excel_data.to_dict('records')
    for year in range(start_year, end_year + 1):
        grd_file = os.path.join(TEMP_DIR, f'Maxtemp_MaxT_{year}.GRD')
        extract_temp_data(grd_file, coordinates, output_folder)
    zip_path = create_zip(output_folder)
    return send_file(zip_path, as_attachment=True)

def create_zip(output_folder):
    """Creates a zip file of the output folder content."""
    zip_path = os.path.join(output_folder, 'output_data.zip')
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, _, files in os.walk(output_folder):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.basename(file_path))
    return zip_path

if __name__ == '__main__':
    app.run(debug=True)
