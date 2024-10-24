from flask import Flask, render_template, jsonify, request, send_file
import requests
import mysql.connector
from mysql.connector import pooling
from datetime import datetime, timezone
import csv
import os
import time
from threading import Thread
import logging
import smtplib
from twilio.rest import Client

# Configurations
API_KEY = "28ca9fb7cc71157af08c49fedd48aa94"
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
DEFAULT_UNIT = 'Celsius'
ALERT_THRESHOLD = 35  # Default threshold in Celsius for alerts
DEFAULT_INTERVAL = 300  # Default interval for weather fetching (5 minutes)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Rathish@123',
    'database': 'weather_data',
    'pool_name': 'weather_pool',
    'pool_size': 5
}

# Twilio Configurations
TWILIO_SID = 'AC35dbe18d736155871cc4cd78bf19accc'
TWILIO_AUTH_TOKEN = '94283fc969c201f137e028984f93498f'
TWILIO_WHATSAPP_NUMBER = 'whatsapp:+14155238886'
USER_WHATSAPP_NUMBER = 'whatsapp:+6383065079'

# Email Configurations
EMAIL_ADDRESS = 'rathishbarathworks@gmail.com'
EMAIL_PASSWORD = 'RathishBarathWorks@123'

# Global preference dictionary
user_preference = {
    'temperature_unit': DEFAULT_UNIT,
    'alert_threshold': ALERT_THRESHOLD,
    'interval': DEFAULT_INTERVAL
}

# Create the Flask app
app = Flask(__name__)

# Set up logging for better debugging and error tracking
logging.basicConfig(level=logging.INFO)

# MySQL Connection Pool
db_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)

# Connect to the MySQL database using the connection pool
def connect_db():
    return db_pool.get_connection()

# Convert temperature based on user preference
def convert_temperature(temp_k, unit):
    if unit == "Celsius":
        return round(temp_k - 273.15, 2)
    elif unit == "Fahrenheit":
        return round((temp_k - 273.15) * 9/5 + 32, 2)
    elif unit == "Kelvin":
        return round(temp_k, 2)
    else:
        return temp_k

# Fetch weather data from OpenWeather API
def fetch_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    try:
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200:
            temp_k = data['main']['temp']
            humidity = data['main']['humidity']
            wind_speed = data['wind']['speed']
            pressure = data['main']['pressure']
            temp_converted = convert_temperature(temp_k, user_preference['temperature_unit'])
            feels_like_converted = convert_temperature(data['main']['feels_like'], user_preference['temperature_unit'])
            weather_data = {
                'city': city,
                'main': data['weather'][0]['main'],
                'temp': temp_converted,
                'feels_like': feels_like_converted,
                'humidity': humidity,
                'wind_speed': wind_speed,
                'pressure': pressure,
                'timestamp': datetime.fromtimestamp(data['dt'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            }
            check_alert(city, temp_converted)
            return weather_data
        else:
            logging.error(f"Failed to fetch weather data for {city}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")
        return None

# Send email notification
def send_email_alert(city, temp):
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        subject = "Weather Alert!"
        body = f"ALERT: Temperature in {city} exceeded {user_preference['alert_threshold']} {user_preference['temperature_unit']}!\nCurrent temperature: {temp} {user_preference['temperature_unit']}"
        msg = f'Subject: {subject}\n\n{body}'
        server.sendmail(EMAIL_ADDRESS, EMAIL_ADDRESS, msg)

# Send WhatsApp notification
def send_whatsapp_alert(city, temp):
    client = Client(TWILIO_SID, TWILIO_AUTH_TOKEN)
    message = f"ALERT: Temperature in {city} exceeded {user_preference['alert_threshold']} {user_preference['temperature_unit']}!\nCurrent temperature: {temp} {user_preference['temperature_unit']}"
    client.messages.create(body=message, from_=TWILIO_WHATSAPP_NUMBER, to=USER_WHATSAPP_NUMBER)

# Check if temperature exceeds the threshold and trigger an alert
def check_alert(city, temp):
    if temp > user_preference['alert_threshold']:
        logging.warning(f"ALERT: Temperature in {city} exceeded {user_preference['alert_threshold']} {user_preference['temperature_unit']}!")
        send_email_alert(city, temp)
        send_whatsapp_alert(city, temp)

# Save weather data to MySQL
def save_weather_data(data):
    try:
        db = connect_db()
        cursor = db.cursor()
        query = """
            INSERT INTO weather (city, main, temp, feels_like, humidity, wind_speed, pressure, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (data['city'], data['main'], data['temp'], data['feels_like'],
                               data['humidity'], data['wind_speed'], data['pressure'], data['timestamp']))
        db.commit()
    except mysql.connector.Error as err:
        logging.error(f"Error saving data to database: {err}")
    finally:
        cursor.close()
        db.close()

# Roll up weather data (daily summary)
def calculate_daily_summary():
    try:
        db = connect_db()
        cursor = db.cursor()
        query = """
            SELECT city, DATE(timestamp) as day, AVG(temp), MAX(temp), MIN(temp),
                   (SELECT main FROM weather w2 WHERE w1.city = w2.city AND DATE(w1.timestamp) = DATE(w2.timestamp)
                    GROUP BY main ORDER BY COUNT(*) DESC LIMIT 1) AS dominant_weather
            FROM weather w1
            GROUP BY city, DATE(timestamp)
        """
        cursor.execute(query)
        daily_summary = cursor.fetchall()
        return daily_summary
    except mysql.connector.Error as err:
        logging.error(f"Error fetching daily summary: {err}")
        return []
    finally:
        cursor.close()
        db.close()

# Fetch and save weather data every interval
def start_weather_monitoring():
    while True:
        for city in CITIES:
            weather_data = fetch_weather_data(city)
            if weather_data:
                save_weather_data(weather_data)
        time.sleep(user_preference['interval'])

# Route for fetching daily summaries and rendering the template
@app.route('/')
def index():
    return render_template('index.html')

# Route for fetching data to display in charts
@app.route('/data')
def data():
    daily_summary = calculate_daily_summary()
    summary_data = [
        {
            'city': row[0],
            'day': row[1].strftime('%Y-%m-%d'),
            'avg_temp': round(row[2], 2),
            'max_temp': row[3],
            'min_temp': row[4],
            'dominant_weather': row[5]
        } for row in daily_summary
    ]
    return jsonify(summary_data)

# Route to set user preferences for temperature unit, alert threshold, and interval
@app.route('/set_preference', methods=['POST'])
def set_preference():
    global user_preference
    data = request.json
    unit = data.get('temperature_unit', DEFAULT_UNIT)
    threshold = data.get('alert_threshold', ALERT_THRESHOLD)
    interval = data.get('interval', DEFAULT_INTERVAL)

    user_preference['temperature_unit'] = unit
    user_preference['alert_threshold'] = threshold
    user_preference['interval'] = interval

    logging.info(f"Updated user preferences: {user_preference}")
    return jsonify({"status": "success", "message": "User preferences updated!"})

# Route to download daily summary as a CSV
@app.route('/download_csv', methods=['GET'])
def download_csv():
    city = request.args.get('city', 'Delhi')
    daily_summary = calculate_daily_summary()
    file_path = f"static/{city}_daily_summary.csv"
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['City', 'Date', 'Average Temp', 'Max Temp', 'Min Temp', 'Dominant Weather'])
        for row in daily_summary:
            if row[0].lower() == city.lower():
                writer.writerow([row[0], row[1].strftime('%Y-%m-%d'), row[2], row[3], row[4], row[5]])

    return send_file(file_path, as_attachment=True)

# Start monitoring in the background
monitoring_thread = Thread(target=start_weather_monitoring)
monitoring_thread.daemon = True
monitoring_thread.start()

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=5000)
