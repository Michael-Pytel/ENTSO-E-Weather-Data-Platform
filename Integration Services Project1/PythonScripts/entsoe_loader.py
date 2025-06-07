# entsoe_loader.py - Pobieranie danych z ENTSO-E
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
import time
import requests
import logging
from utils import (
    setup_logging, ensure_temp_folder, get_last_processed_date, 
    load_dataframe_to_sql, get_date_ranges
)
from config import CONFIG

def get_entsoe_client():
    """Inicjalizacja klienta ENTSO-E"""
    try:
        return EntsoePandasClient(api_key=CONFIG['entsoe_api_key'])
    except Exception as e:
        logging.error(f"Błąd inicjalizacji klienta ENTSO-E: {str(e)}")
        raise
def parse_load_xml(xml_content):
    """Własna funkcja do parsowania XML z odpowiedzi ENTSO-E"""
    try:
        import xml.etree.ElementTree as ET
        from datetime import datetime, timedelta
        import pytz
        import pandas as pd
        
        # Parsowanie XML
        root = ET.fromstring(xml_content)
        
        # Sprawdzenie, czy mamy dokument Acknowledgement (błąd)
        if 'Acknowledgement_MarketDocument' in root.tag:
            # Znajdź elementy Reason
            reasons = root.findall('.//{*}Reason')
            if reasons:
                for reason in reasons:
                    code = reason.find('.//{*}code')
                    text = reason.find('.//{*}text')
                    code_str = code.text if code is not None else "Brak kodu"
                    text_str = text.text if text is not None else "Brak opisu"
                    logging.error(f"Błąd API ENTSO-E: Kod {code_str}, Opis: {text_str}")
            return pd.Series(dtype='float64')  # Pusta seria
        
        # Dla dokumentu GL_MarketDocument (dane prognozy obciążenia)
        if 'GL_MarketDocument' in root.tag:
            # Przygotuj słownik na dane
            data = {}
            
            # Znajdź wszystkie TimeSeries
            time_series = root.findall('.//{*}TimeSeries')
            
            for ts in time_series:
                # Znajdź punkty w szeregu czasowym
                points = ts.findall('.//{*}Point')
                
                # Znajdź okres
                period = ts.find('.//{*}Period')
                if period is None:
                    continue
                
                # Pobierz czas rozpoczęcia
                start_elem = period.find('.//{*}timeInterval/{*}start')
                if start_elem is None:
                    continue
                    
                start_str = start_elem.text
                
                # Obsługa różnych formatów dat ISO
                try:
                    if 'Z' in start_str:
                        start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    else:
                        start_time = datetime.fromisoformat(start_str)
                except ValueError:
                    # Alternatywna metoda parsowania daty
                    from dateutil import parser
                    start_time = parser.parse(start_str)
                
                # Pobierz długość okresu
                resolution_elem = period.find('.//{*}resolution')
                if resolution_elem is None:
                    continue
                    
                resolution = resolution_elem.text
                
                # Konwersja 'PT15M', 'PT60M' itp. na minuty
                minutes = 60  # domyślnie godzina
                if resolution.startswith('PT'):
                    if 'M' in resolution:
                        minutes = int(resolution.replace('PT', '').replace('M', ''))
                    elif 'H' in resolution:
                        minutes = int(resolution.replace('PT', '').replace('H', '')) * 60
                
                # Iteruj przez punkty
                for point in points:
                    position_elem = point.find('.//{*}position')
                    quantity_elem = point.find('.//{*}quantity')
                    
                    if position_elem is None or quantity_elem is None:
                        continue
                        
                    position = int(position_elem.text)
                    quantity = float(quantity_elem.text)
                    
                    # Oblicz rzeczywisty czas dla punktu
                    time_offset = (position - 1) * minutes
                    point_time = start_time + timedelta(minutes=time_offset)
                    
                    # Dodaj do słownika
                    data[point_time] = quantity
            
            # Konwersja do serii pandas
            if data:
                series = pd.Series(data)
                logging.info(f"Pomyślnie sparsowano dane z XML: {len(series)} punktów")
                return series
            else:
                logging.warning("Nie znaleziono danych w odpowiedzi XML.")
                return pd.Series(dtype='float64')
        
        # Domyślnie, jeśli nie rozpoznano formatu
        logging.warning(f"Nieznany format dokumentu XML: {root.tag}")
        return pd.Series(dtype='float64')
    
    except Exception as e:
        logging.error(f"Błąd podczas parsowania XML: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return pd.Series(dtype='float64')

def fetch_load_data(country_code, start_date, end_date, retries=CONFIG['max_retries']):
    """Pobieranie danych o obciążeniu sieci"""
    client = get_entsoe_client()
    
    # Format country_code - sprawdzamy i naprawiamy, jeśli to potrzebne
    if country_code in CONFIG['country_map']:
        country_code = CONFIG['country_map'][country_code]
        logging.info(f"Zamieniono kod kraju na pełny format: {country_code}")
    
    for attempt in range(retries):
        try:
            logging.info(f"Pobieranie danych o obciążeniu dla {country_code} od {start_date} do {end_date} (próba {attempt+1})")
            
            # Zapewnienie, że daty mają informację o strefie czasowej
            if isinstance(start_date, datetime) and start_date.tzinfo is None:
                start_date = pd.Timestamp(start_date).tz_localize('Europe/Warsaw')
            if isinstance(end_date, datetime) and end_date.tzinfo is None:
                end_date = pd.Timestamp(end_date).tz_localize('Europe/Warsaw')
            
            # Wydruk dokładnych parametrów zapytania dla celów diagnostycznych
            logging.info(f"Parametry zapytania: country_code={country_code}, start={start_date.strftime('%Y%m%d%H%M')}, end={end_date.strftime('%Y%m%d%H%M')}")
            
            # Próba pobrania danych poprzez bibliotekę
            try:
                # Pobranie danych o aktualnym obciążeniu
                actual_load = client.query_load(country_code, start=start_date, end=end_date)
                logging.info(f"Pobrano dane o aktualnym obciążeniu: {len(actual_load) if isinstance(actual_load, pd.Series) and not actual_load.empty else 'Nie udało się pobrać'} punktów")
            except Exception as actual_error:
                logging.error(f"Błąd podczas pobierania aktualnego obciążenia: {str(actual_error)}")
                actual_load = pd.Series(dtype='float64')
            
            try:
                # Pobranie danych o prognozowanym obciążeniu
                forecasted_load = client.query_load_forecast(country_code, start=start_date, end=end_date)
                logging.info(f"Pobrano dane o prognozowanym obciążeniu: {len(forecasted_load) if isinstance(forecasted_load, pd.Series) and not forecasted_load.empty else 'Nie udało się pobrać'} punktów")
            except Exception as forecast_error:
                logging.error(f"Błąd podczas pobierania prognozowanego obciążenia metodą biblioteki: {str(forecast_error)}")
                
                # Jeśli biblioteka nie zadziałała, spróbujmy bezpośredniego zapytania
                try:
                    logging.info("Próba bezpośredniego zapytania o prognozę obciążenia...")
                    
                    # Format dat zgodny z API
                    start_str = start_date.strftime('%Y%m%d%H%M')
                    end_str = end_date.strftime('%Y%m%d%H%M')
                    
                    params = {
                        'securityToken': CONFIG['entsoe_api_key'],
                        'documentType': 'A65',  # System total load
                        'processType': 'A01',   # Day ahead (prognoza)
                        'outBiddingZone_Domain': country_code,
                        'periodStart': start_str,
                        'periodEnd': end_str
                    }
                    
                    # Logowanie pełnego URL dla diagnostyki
                    api_url = CONFIG.get('api_endpoint', 'https://web-api.tp.entsoe.eu/api')
                    logging.info(f"Zapytanie do: {api_url} z parametrami: {params}")
                    
                    # Bezpośrednie zapytanie do API
                    response = requests.get(api_url, params=params, timeout=CONFIG['timeout'])
                    
                    # Sprawdzenie statusu odpowiedzi
                    logging.info(f"Kod odpowiedzi: {response.status_code}")
                    if response.status_code == 200:
                        logging.info("Bezpośrednie zapytanie o prognozę zakończone sukcesem")
                        
                        # Zapisz odpowiedź do pliku dla analizy jeśli skonfigurowano
                        if CONFIG.get('save_raw_responses', False):
                            filename = os.path.join(CONFIG['temp_folder'], f"entsoe_response_{country_code}_{start_date.strftime('%Y%m%d')}.xml")
                            with open(filename, 'w', encoding='utf-8') as f:
                                f.write(response.text)
                            logging.info(f"Zapisano pełną odpowiedź do pliku: {filename}")
                        
                        # Użyj własnej funkcji do parsowania XML
                        forecasted_load = parse_load_xml(response.text)
                        
                        if isinstance(forecasted_load, pd.Series) and not forecasted_load.empty:
                            logging.info(f"Pomyślnie sparsowano dane prognozy: {len(forecasted_load)} punktów")
                        else:
                            logging.warning("Odpowiedź nie zawierała oczekiwanych danych prognozy")
                            forecasted_load = pd.Series(dtype='float64')
                    else:
                        logging.error(f"Błąd bezpośredniego zapytania API: Kod {response.status_code}, Treść: {response.text[:300]}")
                        forecasted_load = pd.Series(dtype='float64')
                
                except Exception as direct_error:
                    logging.error(f"Błąd podczas bezpośredniego zapytania o prognozę: {str(direct_error)}")
                    forecasted_load = pd.Series(dtype='float64')
            
            # Sprawdzenie, czy pobrano jakiekolwiek dane
            actual_empty = (not isinstance(actual_load, pd.Series)) or actual_load.empty
            forecast_empty = (not isinstance(forecasted_load, pd.Series)) or forecasted_load.empty
            
            if actual_empty and forecast_empty:
                logging.warning(f"Nie udało się pobrać żadnych danych o obciążeniu dla {country_code}")
                if attempt < retries - 1:
                    sleep_time = 2 ** attempt
                    logging.info(f"Ponowna próba za {sleep_time} sekund...")
                    time.sleep(sleep_time)
                    continue  # Przejdź do następnej próby
                else:
                    return pd.DataFrame()  # Zwróć pusty dataframe po wyczerpaniu prób
            
            # Inicjalizacja pustego dataframe z indeksem czasowym
            all_timestamps = pd.DatetimeIndex([])
            
            # Dodaj wszystkie dostępne punkty czasowe
            if not actual_empty:
                all_timestamps = all_timestamps.union(actual_load.index)
                logging.info(f"Dodano {len(actual_load.index)} punktów czasowych z actual_load")
            if not forecast_empty:
                all_timestamps = all_timestamps.union(forecasted_load.index)
                logging.info(f"Dodano {len(forecasted_load.index)} punktów czasowych z forecasted_load")
            
            # Sprawdź, czy mamy jakiekolwiek punkty czasowe
            if len(all_timestamps) == 0:
                logging.warning("Brak punktów czasowych w danych")
                if attempt < retries - 1:
                    continue  # Spróbuj ponownie
                else:
                    return pd.DataFrame()  # Zwróć pusty dataframe
            
            # Stwórz dataframe ze wszystkimi punktami czasowymi
            load_df = pd.DataFrame(index=all_timestamps)
            
            # Dodaj dane o aktualnym obciążeniu, jeśli dostępne
            if not actual_empty:
                load_df['ActualLoad'] = actual_load
            
            # Dodaj dane o prognozowanym obciążeniu, jeśli dostępne
            if not forecast_empty:
                load_df['ForecastedLoad'] = forecasted_load
            
            # Dodaj brakujące kolumny, jeśli potrzeba
            if 'ActualLoad' not in load_df.columns:
                load_df['ActualLoad'] = np.nan
            
            if 'ForecastedLoad' not in load_df.columns:
                load_df['ForecastedLoad'] = np.nan
            
            # Dodanie kolumny z kodem kraju
            load_df['CountryCode'] = country_code
            
            # Resetowanie indeksu - zamiana indeksu na kolumnę DateTime
            load_df = load_df.reset_index()
            load_df.rename(columns={'index': 'DateTime'}, inplace=True)
            
            # Wydruk przykładowych danych do logów dla celów diagnostycznych
            if not load_df.empty:
                logging.info(f"Pobrano {len(load_df)} rekordów dla {country_code}")
                logging.info(f"Przykładowe dane (pierwsze 3 rekordy):\n{load_df.head(3)}")
                logging.info(f"Liczba wartości NULL: ActualLoad={load_df['ActualLoad'].isna().sum()}, ForecastedLoad={load_df['ForecastedLoad'].isna().sum()}")
                
                # Dodatkowa informacja o liczbie rekordów z danymi
                actual_count = load_df['ActualLoad'].notna().sum()
                forecast_count = load_df['ForecastedLoad'].notna().sum()
                logging.info(f"Liczba rekordów z danymi: ActualLoad={actual_count}, ForecastedLoad={forecast_count}")
            else:
                logging.warning(f"Dataframe jest pusty dla {country_code}")
            
            # Odczekaj między wywołaniami API
            time.sleep(CONFIG['sleep_between_calls'])
            
            return load_df
        
        except Exception as e:
            logging.error(f"Nieoczekiwany błąd podczas pobierania danych o obciążeniu dla {country_code} (próba {attempt+1}): {str(e)}")
            import traceback
            logging.error(f"Szczegóły błędu:\n{traceback.format_exc()}")
            
            if attempt < retries - 1:
                sleep_time = 2 ** attempt
                logging.info(f"Ponowna próba za {sleep_time} sekund...")
                time.sleep(sleep_time)
            else:
                logging.error(f"Wszystkie próby pobrania danych o obciążeniu dla {country_code} zakończyły się niepowodzeniem")
                return pd.DataFrame()

def fetch_generation_data(country_code, start_date, end_date, retries=CONFIG['max_retries']):
    """Pobieranie danych o generacji energii według typu"""
    client = get_entsoe_client()
    
    for attempt in range(retries):
        try:
            logging.info(f"Pobieranie danych o generacji dla {country_code} od {start_date} do {end_date} (próba {attempt+1})")
            
            generation_data = client.query_generation(country_code, start=start_date, end=end_date)
            
            # Sprawdzenie czy są dostępne dane
            if generation_data.empty:
                logging.warning(f"Brak danych generacji dla {country_code} w okresie {start_date} do {end_date}")
                return pd.DataFrame()
            
            # Przekształcenie danych z formatu szerokiego do wąskiego
            # Resetujemy indeks, aby DateTime stał się kolumną
            reset_df = generation_data.reset_index()
            # Zmieniamy nazwę kolumny indeksu na 'DateTime'
            reset_df.rename(columns={'index': 'DateTime'}, inplace=True)
            
            # Przekształcamy z formatu szerokiego do wąskiego
            melted_df = pd.melt(
                reset_df, 
                id_vars=['DateTime'], 
                var_name='ProductionType', 
                value_name='Generation'
            )
            
            # Dodanie kolumny z kodem kraju
            melted_df['CountryCode'] = country_code
            
            # Usunięcie wierszy z wartościami NaN
            melted_df = melted_df.dropna(subset=['Generation'])
            
            time.sleep(CONFIG['sleep_between_calls'])  # Pauza między zapytaniami API
            
            return melted_df
        
        except Exception as e:
            if "No matching data found" in str(e):
                logging.warning(f"Brak danych generacji dla {country_code} w okresie {start_date} do {end_date}")
                return pd.DataFrame()
                
            logging.error(f"Błąd podczas pobierania danych o generacji dla {country_code} (próba {attempt+1}): {str(e)}")
            
            if attempt < retries - 1:
                sleep_time = 2 ** attempt
                logging.info(f"Ponowna próba za {sleep_time} sekund...")
                time.sleep(sleep_time)
            else:
                logging.error(f"Wszystkie próby pobrania danych o generacji dla {country_code} zakończyły się niepowodzeniem")
                return pd.DataFrame()

def fetch_day_ahead_prices(country_code, start_date, end_date, retries=CONFIG['max_retries']):
    """Pobieranie danych o cenach dnia następnego"""
    client = get_entsoe_client()
    
    for attempt in range(retries):
        try:
            logging.info(f"Pobieranie danych o cenach dla {country_code} od {start_date} do {end_date} (próba {attempt+1})")
            
            price_data = client.query_day_ahead_prices(country_code, start=start_date, end=end_date)
            
            # Formatowanie danych
            if isinstance(price_data, pd.Series):
                price_df = pd.DataFrame({'Price': price_data.values}, index=price_data.index)
            else:
                price_df = price_data
            
            # Dodanie kolumny z kodem kraju
            price_df['CountryCode'] = country_code
            
            time.sleep(CONFIG['sleep_between_calls'])  # Pauza między zapytaniami API
            
            return price_df
        
        except Exception as e:
            if "No matching data found" in str(e):
                logging.warning(f"Brak danych cenowych dla {country_code} w okresie {start_date} do {end_date}")
                return pd.DataFrame()
                
            logging.error(f"Błąd podczas pobierania danych o cenach dla {country_code} (próba {attempt+1}): {str(e)}")
            
            if attempt < retries - 1:
                sleep_time = 2 ** attempt
                logging.info(f"Ponowna próba za {sleep_time} sekund...")
                time.sleep(sleep_time)
            else:
                logging.error(f"Wszystkie próby pobrania danych o cenach dla {country_code} zakończyły się niepowodzeniem")
                return pd.DataFrame()

def process_historical_data(data_type, country_code, years_back=CONFIG['historical_years']):
    """Pobieranie historycznych danych"""
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date.replace(year=end_date.year - years_back)
    
    logging.info(f"Rozpoczynam pobieranie historycznych danych {data_type} dla {country_code} od {start_date} do {end_date}")
    
    # Podział zakresu dat na mniejsze przedziały zgodne z ograniczeniami API
    date_ranges = get_date_ranges(start_date, end_date, CONFIG['batch_days'])
    
    all_data = []
    
    for batch_start, batch_end in date_ranges:
        logging.info(f"Przetwarzanie przedziału {batch_start} do {batch_end} dla {country_code}")
        
        # Pobieranie odpowiedniego typu danych
        if data_type == 'load':
            df = fetch_load_data(country_code, batch_start, batch_end)
        elif data_type == 'generation':
            df = fetch_generation_data(country_code, batch_start, batch_end)
        elif data_type == 'price':
            df = fetch_day_ahead_prices(country_code, batch_start, batch_end)
        
        if not df.empty:
            all_data.append(df)
        
        # Zapisanie danych co każde 5 przedziałów (lub gdy jest to ostatni przedział)
        if len(all_data) >= 5 or batch_end >= end_date:
            if all_data:
                combined_df = pd.concat(all_data)
                if not combined_df.empty:
                    # Określenie tabeli docelowej
                    if data_type == 'load':
                        table_name = 'StagingLoad'
                    elif data_type == 'generation':
                        table_name = 'StagingGeneration'
                    elif data_type == 'price':
                        table_name = 'StagingPrice'
                    
                    # Załadowanie do SQL
                    load_dataframe_to_sql(combined_df, table_name)
                    logging.info(f"Zapisano partię danych historycznych {data_type} do bazy")
                
                # Wyczyszczenie listy po zapisie
                all_data = []
    
    logging.info(f"Zakończono pobieranie historycznych danych {data_type} dla {country_code}")

def process_incremental_data(data_type, country_code):
    """Pobieranie przyrostowych danych"""
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    
    # Określenie tabeli źródłowej dla ostatniej daty
    if data_type == 'load':
        table_name = 'StagingLoad'
    elif data_type == 'generation':
        table_name = 'StagingGeneration'
    elif data_type == 'price':
        table_name = 'StagingPrice'
    
    # Pobranie ostatniej przetworzonej daty
    start_date = get_last_processed_date(table_name, country_code)
    
    # Jeśli start_date jest zbyt stare (ponad rok), ogranicz do roku
    year_ago = datetime.now() - timedelta(days=365)
    if start_date < year_ago:
        logging.info(f"Data początkowa ({start_date}) jest starsza niż rok. Ograniczenie do {year_ago}")
        start_date = year_ago
    
    logging.info(f"Pobieranie przyrostowych danych {data_type} dla {country_code} od {start_date} do {end_date}")
    
    # Podział zakresu dat na mniejsze przedziały zgodne z ograniczeniami API
    date_ranges = get_date_ranges(start_date, end_date, CONFIG['batch_days'])
    
    for batch_start, batch_end in date_ranges:
        logging.info(f"Przetwarzanie przedziału {batch_start} do {batch_end} dla {country_code}")
        
        # Pobieranie odpowiedniego typu danych
        if data_type == 'load':
            df = fetch_load_data(country_code, batch_start, batch_end)
        elif data_type == 'generation':
            df = fetch_generation_data(country_code, batch_start, batch_end)
        elif data_type == 'price':
            df = fetch_day_ahead_prices(country_code, batch_start, batch_end)
        
        if not df.empty:
            # Załadowanie do SQL
            load_dataframe_to_sql(df, table_name)
            logging.info(f"Zapisano dane przyrostowe {data_type} dla {country_code} do bazy")

def process_entsoe_data(data_type, country_code=None, is_historical=False, start_date=None, end_date=None):
    """Główna funkcja do przetwarzania danych ENTSO-E"""
    setup_logging('entsoe_loader')
    ensure_temp_folder()
    
    logging.info(f"Rozpoczynam proces_entsoe_data z parametrami: typ={data_type}, kraj={country_code}, historyczne={is_historical}")
    
    # Określenie listy krajów do przetworzenia
    countries_to_process = []
    if country_code:
        # Pojedynczy kraj
        countries_to_process.append(country_code)
    else:
        # Wszystkie kraje z konfiguracji
        countries_to_process = [zone['code'] for zone in CONFIG['bidding_zones']]
    
    for current_country in countries_to_process:
        try:
            logging.info(f"Przetwarzanie danych dla {current_country}")
            
            if is_historical:
                # Przetwarzanie danych historycznych
                process_historical_data(data_type, current_country)
            elif start_date and end_date:
                # Przetwarzanie dla konkretnego zakresu dat
                if isinstance(start_date, str):
                    start_date = pd.Timestamp(start_date)
                if isinstance(end_date, str):
                    end_date = pd.Timestamp(end_date)
                
                # Dodaj strefę czasową, jeśli nie została określona
                if start_date.tzinfo is None:
                    start_date = start_date.tz_localize('Europe/Warsaw')
                if end_date.tzinfo is None:
                    end_date = end_date.tz_localize('Europe/Warsaw')
                
                logging.info(f"Przetwarzanie danych dla zakresu {start_date} do {end_date}")
                
                # Podział zakresu dat na mniejsze przedziały
                date_ranges = get_date_ranges(start_date, end_date, CONFIG['batch_days'])
                
                for batch_start, batch_end in date_ranges:
                    logging.info(f"Przetwarzanie przedziału {batch_start} do {batch_end}")
                    
                    # Pobieranie danych
                    if data_type == 'load':
                        df = fetch_load_data(current_country, batch_start, batch_end)
                        table_name = 'StagingLoad'
                    elif data_type == 'generation':
                        df = fetch_generation_data(current_country, batch_start, batch_end)
                        table_name = 'StagingGeneration'
                    elif data_type == 'price':
                        df = fetch_day_ahead_prices(current_country, batch_start, batch_end)
                        table_name = 'StagingPrice'
                    
                    if not df.empty:
                        # Załadowanie do SQL
                        load_dataframe_to_sql(df, table_name)
                        logging.info(f"Zapisano dane {data_type} dla {current_country} do bazy")
            else:
                # Przetwarzanie przyrostowe (najnowsze dane)
                process_incremental_data(data_type, current_country)
        
        except Exception as e:
            logging.error(f"Błąd podczas przetwarzania {data_type} dla {current_country}: {str(e)}")
            # Kontynuuj z następnym krajem
    
    logging.info(f"Zakończono przetwarzanie danych {data_type}")
    return 0

if __name__ == "__main__":
    # Parsowanie argumentów wiersza poleceń
    import argparse
    
    parser = argparse.ArgumentParser(description='Pobieranie danych z ENTSO-E')
    parser.add_argument('data_type', choices=['load', 'generation', 'price'], help='Typ danych do pobrania')
    parser.add_argument('--country', '-c', help='Kod kraju/strefy ofertowej (domyślnie: wszystkie)')
    parser.add_argument('--historical', '-hist', action='store_true', help='Pobierz dane historyczne')
    parser.add_argument('--start', '-s', help='Data początkowa (format: YYYY-MM-DD)')
    parser.add_argument('--end', '-e', help='Data końcowa (format: YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Konwersja dat, jeśli podano
    start_date = None
    end_date = None
    
    if args.start:
        start_date = pd.Timestamp(args.start)
    if args.end:
        end_date = pd.Timestamp(args.end)
    
    sys.exit(process_entsoe_data(
        args.data_type, 
        country_code=args.country, 
        is_historical=args.historical,
        start_date=start_date,
        end_date=end_date
    ))