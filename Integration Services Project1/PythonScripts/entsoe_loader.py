# entsoe_loader.py - Pobieranie danych z ENTSO-E
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
from utils import ensure_temp_folder, get_last_processed_date, load_dataframe_to_sql
from config import CONFIG
import logging

logging.basicConfig(
    filename='D:/hurtownie/Integration Services Project1/logs/entsoe_loader_ssis.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


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
            load_df = pd.DataFrame({'Actual Load': load_data.values}, index=load_data.index)
        else:
            load_df = load_data
        
        # Obsługa duplikatów indeksu (dat)
        if not load_df.index.is_unique:
            print("Wykryto duplikaty dat w danych - obsługa zmiany czasu...")
            # Metoda 1: Zostawienie tylko pierwszego wystąpienia
            load_df = load_df.loc[~load_df.index.duplicated(keep='first')]
            
            # Alternatywnie, można uśrednić wartości dla duplikatów:
            # load_df = load_df.groupby(level=0).mean()
        
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
        
        # Przekształcenie danych z formatu szerokiego do wąskiego
        # generation_data to DataFrame z indeksem DateTime i kolumnami typów produkcji
        
        # Resetujemy indeks, aby DateTime stał się kolumną
        reset_df = generation_data.reset_index()
        # Zmieniamy nazwę kolumny indeksu na 'DateTime'
        reset_df.rename(columns={'index': 'DateTime'}, inplace=True)
        
        # Przekształcamy z formatu szerokiego do wąskiego
        # Wszystkie kolumny oprócz DateTime staną się wierszami
        melted_df = pd.melt(
            reset_df, 
            id_vars=['DateTime'], 
            var_name='ProductionType', 
            value_name='Generation'
        )
        
        # Usunięcie wierszy z wartościami NaN
        melted_df = melted_df.dropna(subset=['Generation'])
        
        return melted_df
    except Exception as e:
        logging.error(f"Błąd podczas pobierania danych o generacji: {str(e)}")
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
    # Diagnostyczne informacje o środowisku
    logging.info(f"Working directory: {os.getcwd()}")
    logging.info(f"Python path: {sys.executable}")
    try:
        logging.info(f"Files in current directory: {os.listdir('.')}")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        logging.info(f"Script directory: {script_dir}")
        logging.info(f"Files in script directory: {os.listdir(script_dir)}")
    except Exception as e:
        logging.error(f"Error listing directory contents: {str(e)}")
    
    ensure_temp_folder()
    logging.info(f"Starting process_entsoe_data with type={data_type}, days_back={days_back}")
    logging.info(f"Script arguments: {sys.argv}")
    
    # Ustawienie dat
    try:
        if len(sys.argv) >= 4:  # Format: script.py data_type start_date end_date
            logging.info(f"Using args[2] and args[3] for dates")
            start_date = pd.Timestamp(sys.argv[2])
            end_date = pd.Timestamp(sys.argv[3])
        elif len(sys.argv) == 3:  # Format: script.py start_date end_date
            logging.info(f"Using args[1] and args[2] for dates")
            start_date = pd.Timestamp(sys.argv[1])
            end_date = pd.Timestamp(sys.argv[2])
        else:
            # Domyślnie ostatnich X dni
            logging.info(f"Using default date range (last {days_back} days)")
            end_date = pd.Timestamp('now').replace(hour=0, minute=0, second=0)
            start_date = end_date - timedelta(days=days_back)
        
        # Dodaj strefę czasową, jeśli nie została określona
        if start_date.tzinfo is None:
            start_date = start_date.tz_localize('Europe/Warsaw')
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize('Europe/Warsaw')
            
        logging.info(f"Final date range: {start_date} to {end_date}")
    except Exception as e:
        logging.error(f"Error parsing dates: {str(e)}")
        # Fallback do bezpiecznych dat
        end_date = pd.Timestamp('now', tz='Europe/Warsaw').replace(hour=0, minute=0, second=0)
        start_date = end_date - timedelta(days=days_back)
        logging.info(f"Fallback date range: {start_date} to {end_date}")
    
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