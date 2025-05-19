# weather_loader.py - Pobieranie danych pogodowych z Open-Meteo
import sys
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from utils import ensure_temp_folder, get_last_processed_date, load_dataframe_to_sql
from config import CONFIG

def fetch_weather_data(location, start_date, end_date):
    """Pobieranie danych pogodowych dla danej lokalizacji"""
    try:
        # Formatowanie dat do formatu YYYY-MM-DD
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Tworzenie URL API
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={location['latitude']}&longitude={location['longitude']}&start_date={start_str}&end_date={end_str}&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,cloud_cover,shortwave_radiation"
        
        # Wykonanie zapytania
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Błąd API Open-Meteo: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        
        # Konwersja do DataFrame
        hourly_data = pd.DataFrame({
            'Location': location['name'],
            'time': pd.to_datetime(data['hourly']['time']),
            'temperature': data['hourly']['temperature_2m'],
            'humidity': data['hourly']['relative_humidity_2m'],
            'precipitation': data['hourly']['precipitation'],
            'wind_speed': data['hourly']['wind_speed_10m'],
            'wind_direction': data['hourly']['wind_direction_10m'],
            'cloud_cover': data['hourly']['cloud_cover'],
            'radiation': data['hourly']['shortwave_radiation']
        })
        
        return hourly_data
    except Exception as e:
        print(f"Błąd podczas pobierania danych pogodowych: {str(e)}")
        return pd.DataFrame()

def process_weather_data(days_back=10):
    """Główna funkcja do przetwarzania danych pogodowych"""
    ensure_temp_folder()
    
    # Ustawienie dat
    if len(sys.argv) >= 3:
        start_date = pd.Timestamp(sys.argv[1])
        end_date = pd.Timestamp(sys.argv[2])
    else:
        # Domyślnie ostatnich X dni
        end_date = pd.Timestamp('now').replace(hour=0, minute=0, second=0)
        start_date = end_date - timedelta(days=days_back)
    
    print(f"Przetwarzanie danych pogodowych od {start_date} do {end_date}")
    
    # Lista DataFrame'ów dla poszczególnych lokalizacji
    dfs = []
    
    # Pobieranie danych dla każdej lokalizacji
    for location in CONFIG['locations']:
        print(f"Pobieranie danych dla lokalizacji: {location['name']}")
        df = fetch_weather_data(location, start_date, end_date)
        if not df.empty:
            dfs.append(df)
    
    # Połączenie wszystkich danych
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Zapisanie do pliku CSV (opcjonalnie)
        csv_path = os.path.join(CONFIG['temp_folder'], "weather_data.csv")
        combined_df.to_csv(csv_path, index=False)
        print(f"Zapisano dane do {csv_path}")
        
        # Załadowanie do SQL
        load_dataframe_to_sql(combined_df, 'StagingWeather')
        print("Załadowano dane pogodowe do tabeli StagingWeather")
    else:
        print("Nie udało się pobrać żadnych danych pogodowych. Kończenie.")
        sys.exit(1)
    
    return 0

if __name__ == "__main__":
    sys.exit(process_weather_data())