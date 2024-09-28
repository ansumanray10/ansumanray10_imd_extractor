from flask import Flask, render_template, request, send_file, flash
import os
import netCDF4 as nc
import numpy as np
import pandas as pd
import io
import json
import shutil
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Required for flashing messages

# Define the credentials and Google Drive folder ID
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_FOLDER_ID = '1kQpXiQq1B845w6JpxN2SgCeQi9MxItA4'

# Read Google service account credentials from environment variable
service_account_info = json.loads(os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

# Initialize Google Drive API
drive_service = build('drive', 'v3', credentials=credentials)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit():
    year_type = request.form.get('yearType')
    coord_type = request.form.get('coordType')
    
    try:
        # Handle both single year and range of years
        if coord_type == 'single':
            latitude = request.form.get('latitude')
            longitude = request.form.get('longitude')

            if year_type == 'single':
                year = request.form.get('year')
                if year and latitude and longitude:
                    process_and_send_single_coordinate(year, latitude, longitude)
                else:
                    flash("Please provide valid inputs for year, latitude, and longitude.")
                    return render_template('index.html')

            else:  # Range of years
                start_year = request.form.get('start_year')
                end_year = request.form.get('end_year')
                if start_year and end_year and latitude and longitude:
                    process_range_of_years_for_single_coordinate(start_year, end_year, latitude, longitude)
                else:
                    flash("Please provide valid inputs for start year, end year, latitude, and longitude.")
                    return render_template('index.html')

        # Process Excel file with multiple coordinates
        elif coord_type == 'excel':
            coordinate_file = request.files.get('coordinateFile')
            if coordinate_file:
                excel_data = pd.read_excel(coordinate_file)
                if 'Latitude' not in excel_data.columns or 'Longitude' not in excel_data.columns:
                    flash("Excel file must contain 'Latitude' and 'Longitude' columns.")
                    return render_template('index.html')
                
                if year_type == 'single':
                    year = request.form.get('year')
                    process_all_coordinates_for_single_year(year, excel_data)

                else:  # Range of years
                    start_year = request.form.get('start_year')
                    end_year = request.form.get('end_year')
                    if start_year and end_year:
                        process_all_coordinates_for_range_of_years(start_year, end_year, excel_data)
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

def process_and_send_single_coordinate(year, latitude, longitude):
    """Process a single coordinate for a single year."""
    data_frames = []
    if process_nc_file_from_drive(year, float(latitude), float(longitude), data_frames):
        return prepare_and_send_response(data_frames, latitude, longitude, year)
    else:
        flash(f"Failed to process data for year {year}.")
        return render_template('index.html')

def process_range_of_years_for_single_coordinate(start_year, end_year, latitude, longitude):
    """Process a single coordinate for a range of years."""
    data_frames = []
    start_year = int(start_year)
    end_year = int(end_year)
    for year in range(start_year, end_year + 1):
        process_nc_file_from_drive(year, float(latitude), float(longitude), data_frames)

    if data_frames:
        return prepare_and_send_response(data_frames, latitude, longitude, f'{start_year}-{end_year}', is_range=True)
    else:
        flash(f"No data found for years {start_year}-{end_year}.")
        return render_template('index.html')

def process_all_coordinates_for_single_year(year, excel_data):
    """Processes all coordinates in the Excel file for a single year."""
    file_id = get_nc_file_id_from_drive(year)
    if file_id:
        file_path = download_nc_file_from_drive(file_id, year)
        for _, row in excel_data.iterrows():
            latitude = row['Latitude']
            longitude = row['Longitude']
            process_coordinate_for_single_year(file_path, year, latitude, longitude)

def process_all_coordinates_for_range_of_years(start_year, end_year, excel_data):
    """Processes all coordinates for a range of years."""
    start_year = int(start_year)
    end_year = int(end_year)
    for year in range(start_year, end_year + 1):
        file_id = get_nc_file_id_from_drive(year)
        if file_id:
            file_path = download_nc_file_from_drive(file_id, year)
            for _, row in excel_data.iterrows():
                latitude = row['Latitude']
                longitude = row['Longitude']
                process_coordinate_for_single_year(file_path, year, latitude, longitude)

def process_coordinate_for_single_year(file_path, year, latitude, longitude):
    """Processes a single coordinate for a single year and saves the data."""
    df = extract_rainfall_data(file_path, float(latitude), float(longitude), year)
    if isinstance(df, pd.DataFrame) and not df.empty:
        output_file = os.path.join('temp_nc_files', f'rainfall_data_{latitude}_{longitude}_{year}.xlsx')
        df.to_excel(output_file, index=False)
        print(f"Saved data for {latitude}, {longitude} for year {year}.")
    else:
        print(f"No data found for {latitude}, {longitude} for year {year}.")

def prepare_and_send_response(data_frames, latitude, longitude, year, is_range=False):
    """Prepares the response by concatenating data and sending the output as an Excel file."""
    try:
        result_df = pd.concat(data_frames)
        output_file = os.path.join('temp_nc_files', f'rainfall_data_{latitude}_{longitude}_range_{year}.xlsx') if is_range else \
                      os.path.join('temp_nc_files', f'rainfall_data_{latitude}_{longitude}_{year}.xlsx')
        result_df.to_excel(output_file, index=False)
        return send_file(output_file, as_attachment=True)
    finally:
        shutil.rmtree('temp_nc_files', ignore_errors=True)

if __name__ == '__main__':
    app.run(debug=True)
