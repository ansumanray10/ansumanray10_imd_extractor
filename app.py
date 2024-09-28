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
        # Single coordinate type
        if coord_type == 'single':
            latitude = request.form.get('latitude')
            longitude = request.form.get('longitude')

            if year_type == 'single':
                year = request.form.get('year')
                if year and latitude and longitude:
                    data_frames = []
                    if process_nc_file_from_drive(year, float(latitude), float(longitude), data_frames):
                        return prepare_and_send_response(data_frames, latitude, longitude, year)
                    else:
                        flash(f"Failed to process data for year {year}.")
                        return render_template('index.html')
                else:
                    flash("Please provide valid inputs for year, latitude, and longitude.")
                    return render_template('index.html')

            else:  # Range of years
                start_year = request.form.get('start_year')
                end_year = request.form.get('end_year')
                if start_year and end_year and latitude and longitude:
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
                else:
                    flash("Please provide valid inputs for start year, end year, latitude, and longitude.")
                    return render_template('index.html')
        
        # Excel file with multiple coordinates
        elif coord_type == 'excel':
            coordinate_file = request.files.get('coordinateFile')
            if coordinate_file:
                excel_data = pd.read_excel(coordinate_file)
                if 'Latitude' not in excel_data.columns or 'Longitude' not in excel_data.columns:
                    flash("Excel file must contain 'Latitude' and 'Longitude' columns.")
                    return render_template('index.html')
                
                data_frames = []
                
                if year_type == 'single':
                    year = request.form.get('year')
                    for _, row in excel_data.iterrows():
                        process_nc_file_from_drive(year, row['Latitude'], row['Longitude'], data_frames)
                    
                    if data_frames:
                        return prepare_and_send_response(data_frames, 'multiple', 'multiple', year)
                    else:
                        flash(f"No data found for year {year}.")
                        return render_template('index.html')

                else:  # Range of years
                    start_year = request.form.get('start_year')
                    end_year = request.form.get('end_year')
                    if start_year and end_year:
                        start_year = int(start_year)
                        end_year = int(end_year)
                        for year in range(start_year, end_year + 1):
                            for _, row in excel_data.iterrows():
                                process_nc_file_from_drive(year, row['Latitude'], row['Longitude'], data_frames)
                        
                        if data_frames:
                            return prepare_and_send_response(data_frames, 'multiple', 'multiple', f'{start_year}-{end_year}', is_range=True)
                        else:
                            flash(f"No data found for years {start_year}-{end_year}.")
                            return render_template('index.html')
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

# Function to process NetCDF files for specific years
def process_nc_file_from_drive(year, latitude, longitude, data_frames):
    """Processes the NetCDF file from Google Drive for the given year."""
    try:
        file_id = get_nc_file_id_from_drive(year)
        if file_id:
            file_path = download_nc_file_from_drive(file_id, year)
            df = extract_rainfall_data(file_path, latitude, longitude, year)
            if isinstance(df, pd.DataFrame) and not df.empty:
                data_frames.append(df)
                return True
        return False
    except Exception as e:
        print(f"Error processing file from drive: {e}")
        return False

# Function to download files from Google Drive
def download_nc_file_from_drive(file_id, year):
    """Downloads the NetCDF file from Google Drive to a local temp folder."""
    try:
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
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

# Function to get the NetCDF file ID from Google Drive
def get_nc_file_id_from_drive(year):
    """Gets the file ID for a specific year from the Google Drive folder."""
    try:
        query = f"'{DRIVE_FOLDER_ID}' in parents and name contains '{year}' and mimeType='application/x-netcdf'"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files:
            return files[0]['id']  # Return the first file found for the year
        return None
    except Exception as e:
        print(f"Error fetching file ID: {e}")
        return None

# Function to extract rainfall data from the NetCDF file
def extract_rainfall_data(file_path, target_lat, target_lon, year):
    """Extracts the rainfall data for the given coordinates from the stored NetCDF file."""
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

# Function to prepare and send the response as an Excel file
def prepare_and_send_response(data_frames, latitude, longitude, year, is_range=False):
    """Prepares the response by concatenating data and sending the output as an Excel file."""
    try:
        result_df = pd.concat(data_frames)
        output_file = os.path.join('temp_nc_files', f'rainfall_data_{latitude}_{longitude}_range_{year}.xlsx') if is_range else \
                      os.path.join('temp_nc_files', f'rainfall_data_{latitude}_{longitude}_{year}.xlsx')
        result_df.to_excel(output_file, index=False)
        return send_file(output_file, as_attachment=True)
    finally:
        # Cleanup: Remove temporary files after response is prepared
        shutil.rmtree('temp_nc_files', ignore_errors=True)

if __name__ == '__main__':
    app.run(debug=True)