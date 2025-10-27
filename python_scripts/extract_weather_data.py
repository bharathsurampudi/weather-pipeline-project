import requests
import json
import os
import datetime
import sys

# --- Configuration ---
# Replace with your actual OpenWeatherMap API key
# IMPORTANT: In a real Airflow DAG, use Connections or Variables, not hardcoding!
API_KEY = "68337681cc251db3a87e4780847b278e" 

# Example Australian City IDs (Find IDs here: https://openweathermap.org/find)
# Sydney: 2147714, Melbourne: 2158177, Brisbane: 2174003, Perth: 2063523
CITY_IDS = {
    "sydney": "2147714",
    "melbourne": "2158177",
    "brisbane": "2174003",
    "perth": "2063523",
}

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Define where to save the output files (Airflow often uses /tmp or a specific data dir)
OUTPUT_DIR = "/tmp/weather_data" 

# --- Functions ---
def get_weather_data(city_id, api_key):
    """Fetches current weather data for a given city ID."""
    params = {
        'id': city_id,
        'appid': api_key,
        'units': 'metric' # Use metric units (Celsius)
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        print(f"Successfully fetched data for city ID {city_id}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for city ID {city_id}: {e}")
        return None

def save_data_to_json(data, city_name, output_dir):
    """Saves the weather data to a JSON file."""
    if data is None:
        print(f"No data to save for {city_name}.")
        return None

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get current date for filename
    current_date = datetime.datetime.now().strftime('%Y%m%d')
    file_path = os.path.join(output_dir, f"weather_{city_name}_{current_date}.json")

    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Successfully saved data for {city_name} to {file_path}")
        return file_path
    except IOError as e:
        print(f"Error saving data for {city_name} to {file_path}: {e}")
        return None

# --- Main Execution ---
def main():
    """Main function to fetch and save weather data for configured cities."""
    if API_KEY == "YOUR_API_KEY_HERE":
        print("ERROR: Please replace 'YOUR_API_KEY_HERE' with your actual OpenWeatherMap API key.")
        sys.exit(1) # Exit with an error code

    saved_files = []
    for city_name, city_id in CITY_IDS.items():
        weather_data = get_weather_data(city_id, API_KEY)
        file_path = save_data_to_json(weather_data, city_name, OUTPUT_DIR)
        if file_path:
            saved_files.append(file_path)

    print("\n--- Summary ---")
    if saved_files:
        print("Saved weather data files:")
        for file in saved_files:
            print(f"- {file}")
    else:
        print("No weather data files were saved.")

if __name__ == "__main__":
    main()