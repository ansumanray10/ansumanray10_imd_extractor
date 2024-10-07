from flask import Flask, request, render_template, send_file, flash
from rainfall_extractor import submit_rainfall  # Import from rainfall_extractor.py
from temp_extractor import submit_temperature  # Import from temp_extractor.py

app = Flask(__name__)
app.secret_key = 'bbc887a4a32e9d3d0725ce0d1eef1f2b'  # Use your generated secret key

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
    # Get data type (Rainfall or Temperature) selected by the user
    data_type = request.form.get('dataType')

    # Route the request to the appropriate extractor based on the data type
    if data_type == 'rainfall':
        return submit_rainfall()  # Call the rainfall processing function
    elif data_type == 'temperature':
        return submit_temperature()  # Call the temperature processing function
    else:
        flash("Invalid data type selected.")
        return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
