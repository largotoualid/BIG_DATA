import requests
import csv
import time

start_year = 1979
end_year = 2024

latitude = 40.7128    # New York City for example
longitude = -74.0060

columns = ["date", "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "windspeed_10m_max"]
weather_data = []

for year in range(start_year, end_year + 1):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={latitude}&longitude={longitude}"
        f"&start_date={year}-01-01&end_date={year}-12-31"
        f"&daily={','.join(columns[1:])}&timezone=auto"
    )

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "daily" in data:
            days = data["daily"]["time"]
            for i in range(len(days)):
                row = [days[i]]
                for col in columns[1:]:
                    row.append(data["daily"][col][i])
                weather_data.append(row)

        print(f"Extracted year {year}, total rows so far: {len(weather_data)}")
        time.sleep(1)

    except Exception as e:
        print(f"Error in year {year}: {e}")
        time.sleep(3)

# Save to CSV
with open("weather_data.csv", "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(columns)
    writer.writerows(weather_data)

print(f"Total {len(weather_data)} rows saved to 'weather_data.csv'")
