# entsoe_loader.py - Pobieranie danych z ENTSO-E
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
from utils import ensure_temp_folder, get_last_processed_date, load_dataframe_to_sql
from config import CONFIG

def get_entsoe_client():
    """Inicjalizacja klienta ENTSO-E"""
    try:
        return EntsoePandasClient(api_key=CONFIG['entsoe_api_key'])
    except Exception as e:
        print(f"Błąd inicjalizacji klienta ENTSO-E: {str(e)}")
        sys.exit(1)

def fetch_load_data(start_date, end_date):
    """Pobieranie danych o obciążeniu sieci"""
    client = get_entsoe_client()
    country_code = CONFIG['country_code']
    
    try:
        load_data = client.query_load(country_code, start=start_date, end=end_date)
        
        # Formatowanie danych
        if isinstance(load_data, pd.Series):
            load_df = pd.DataFrame({'Load': load_data.values}, index=load_data.index)
        else:
            load_df = load_data
        
        return load_df
    except Exception as e:
        print(f"Błąd podczas pobierania danych o obciążeniu: {str(e)}")
        return pd.DataFrame()

def fetch_generation_data(start_date, end_date):
    """Pobieranie danych o generacji energii według typu"""
    client = get_entsoe_client()
    country_code = CONFIG['country_code']
    
    try:
        generation_data = client.query_generation(country_code, start=start_date, end=end_date)
        return generation_data
    except Exception as e:
        print(f"Błąd podczas pobierania danych o generacji: {str(e)}")
        return pd.DataFrame()

def fetch_day_ahead_prices(start_date, end_date):
    """Pobieranie danych o cenach dnia następnego"""
    client = get_entsoe_client()
    country_code = CONFIG['country_code']
    
    try:
        price_data = client.query_day_ahead_prices(country_code, start=start_date, end=end_date)
        
        # Formatowanie danych
        if isinstance(price_data, pd.Series):
            price_df = pd.DataFrame({'Price': price_data.values}, index=price_data.index)
        else:
            price_df = price_data
        
        return price_df
    except Exception as e:
        print(f"Błąd podczas pobierania danych o cenach: {str(e)}")
        return pd.DataFrame()

def process_entsoe_data(data_type, days_back=10):
    """Główna funkcja do przetwarzania danych ENTSO-E"""
    ensure_temp_folder()
    
    # Ustawienie dat
    if len(sys.argv) >= 3:
        start_date = pd.Timestamp(sys.argv[1], tz='Europe/Warsaw')
        end_date = pd.Timestamp(sys.argv[2], tz='Europe/Warsaw')
    else:
        # Domyślnie ostatnich X dni
        end_date = pd.Timestamp('now', tz='Europe/Warsaw').replace(hour=0, minute=0, second=0)
        start_date = end_date - timedelta(days=days_back)
    
    print(f"Przetwarzanie danych {data_type} od {start_date} do {end_date}")
    
    # Pobieranie danych
    if data_type == 'load':
        df = fetch_load_data(start_date, end_date)
        table_name = 'StagingLoad'
    elif data_type == 'generation':
        df = fetch_generation_data(start_date, end_date)
        table_name = 'StagingGeneration'
    elif data_type == 'price':
        df = fetch_day_ahead_prices(start_date, end_date)
        table_name = 'StagingPrice'
    else:
        print(f"Nieznany typ danych: {data_type}")
        sys.exit(1)
    
    # Sprawdzenie czy pobranie danych się powiodło
    if df.empty:
        print(f"Nie udało się pobrać danych {data_type}. Kończenie.")
        sys.exit(1)
    
    # Zapisanie do pliku CSV (opcjonalnie)
    csv_path = os.path.join(CONFIG['temp_folder'], f"{data_type}_data.csv")
    df.to_csv(csv_path)
    print(f"Zapisano dane do {csv_path}")
    
    # Załadowanie do SQL
    load_dataframe_to_sql(df, table_name)
    print(f"Załadowano dane {data_type} do tabeli {table_name}")
    
    return 0

if __name__ == "__main__":
    # Sprawdzenie argumentów
    if len(sys.argv) < 2:
        print("Użycie: python entsoe_loader.py <typ_danych> [data_początkowa] [data_końcowa]")
        print("gdzie typ_danych to: load, generation lub price")
        sys.exit(1)
    
    data_type = sys.argv[1].lower()
    sys.exit(process_entsoe_data(data_type))