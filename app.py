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
app.secret_key = 'supersecretkey'

# Google Drive Setup
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
DRIVE_FOLDER_ID = '1kQpXiQq1B845w6JpxN2SgCeQi9MxItA4'
service_account_info = json.loads(os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit():
    year_type = request.form.get('yearType')
    coord_type = request.form.get('coordType')

    try:
        # Single coordinate, single year
        if coord_type == 'single':
            latitude = float(request.form.get('latitude'))
            longitude = float(request.form.get('longitude'))

            if year_type == 'single':
                year = request.form.get('year')
                process_single_point_single_year(latitude, longitude, year)
            else:  # Range of years
                start_year = int(request.form.get('start_year'))
                end_year = int(request.form.get('end_year'))
                process_single_point_range_years(latitude, longitude, start_year, end_year)

        # Excel coordinates
        elif coord_type == 'excel':
            coordinate_file = request.files.get('coordinateFile')
            if coordinate_file:
                excel_data = pd.read_excel(coordinate_file)

                if year_type == 'single':
                    year = request.form.get('year')
                    process_multiple_points_single_year(excel_data, year)
                else:  # Range of years
                    start_year = int(request.form.get('start_year'))
                    end_year = int(request.form.get('end_year'))
                    process_multiple_points_range_years(excel_data, start_year, end_year)

        return render_template('index.html')
    except Exception as e:
        print(f"Error: {e}")
        flash(f"An error occurred: {e}")
        return render_template('index.html')


def process_single_point_single_year(latitude, longitude, year):
    """ Process a single point for a single year """
    data_frames = []
    process_nc_file_from_drive(year, latitude, longitude, data_frames)
    return prepare_and_send_response(data_frames, latitude, longitude, year)


def process_single_point_range_years(latitude, longitude, start_year, end_year):
    """ Process a single point for a range of years """
    for year in range(start_year, end_year + 1):
        data_frames = []
        process_nc_file_from_drive(year, latitude, longitude, data_frames)
        prepare_and_send_response(data_frames, latitude, longitude, year)


def process_multiple_points_single_year(excel_data, year):
    """ Process multiple points from Excel for a single year """
    for _, row in excel_data.iterrows():
        data_frames = []
        process_nc_file_from_drive(year, row['Latitude'], row['Longitude'], data_frames)
        prepare_and_send_response(data_frames, row['Latitude'], row['Longitude'], year)


def process_multiple_points_range_years(excel_data, start_year, end_year):
    """ Process multiple points from Excel for a range of years """
    for year in range(start_year, end_year + 1):
        for _, row in excel_data.iterrows():
            data_frames = []
            process_nc_file_from_drive(year, row['Latitude'], row['Longitude'], data_frames)
            prepare_and_send_response(data_frames, row['Latitude'], row['Longitude'], year)


def process_nc_file_from_drive(year, latitude, longitude, data_frames):
    """ Process NetCDF file from Google Drive """
    file_id = get_nc_file_id_from_drive(year)
    if file_id:
        file_path = download_nc_file_from_drive(file_id, year)
        df = extract_rainfall_data(file_path, latitude, longitude, year)
        if isinstance(df, pd.DataFrame) and not df.empty:
            data_frames.append(df)
            return True
    return False


def download_nc_file_from_drive(file_id, year):
    """ Download NetCDF file """
    request = drive_service.files().get_media(fileId=file_id)
    file_path = os.path.join('temp_nc_files', f'rainfall_{year}.nc')
    if not os.path.exists('temp_nc_files'):
        os.makedirs('temp_nc_files')
    with open(file_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading file: {int(status.progress() * 100)}% complete.")
    return file_path


def get_nc_file_id_from_drive(year):
    """ Get the file ID for a specific year from Google Drive """
    query = f"'{DRIVE_FOLDER_ID}' in parents and name contains '{year}' and mimeType='application/x-netcdf'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if files:
        return files[0]['id']  # Return the first file found for the year
    return None


def extract_rainfall_data(file_path, target_lat, target_lon, year):
    """ Extract rainfall data for given coordinates from NetCDF file """
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
    return None


def prepare_and_send_response(data_frames, latitude, longitude, year):
    """ Prepare response by sending the extracted data as an Excel file """
    result_df = pd.concat(data_frames)
    output_file = os.path.join('temp_nc_files', f'rainfall_data_{latitude}_{longitude}_{year}.xlsx')
    result_df.to_excel(output_file, index=False)
    return send_file(output_file, as_attachment=True)
