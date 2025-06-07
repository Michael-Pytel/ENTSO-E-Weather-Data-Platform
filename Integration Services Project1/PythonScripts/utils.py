# utils.py - Wspólne funkcje używane w skryptach
import os
import sys
import pandas as pd
import pyodbc
import logging
from datetime import datetime, timedelta
import time
from config import CONFIG

def setup_logging(script_name):
    """Konfiguracja logowania"""
    if not os.path.exists(CONFIG['log_folder']):
        os.makedirs(CONFIG['log_folder'])
    
    log_file = os.path.join(CONFIG['log_folder'], f"{script_name}_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Dodanie logowania do konsoli
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

def connect_to_sql():
    """Nawiązanie połączenia z bazą danych SQL Server"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={CONFIG['sql_server']};"
            f"DATABASE={CONFIG['sql_database']};"
            f"UID={CONFIG['sql_username']};"
            f"PWD={CONFIG['sql_password']}"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Błąd podczas łączenia z bazą danych: {str(e)}")
        raise

def ensure_temp_folder():
    """Zapewnienie istnienia folderu tymczasowego"""
    if not os.path.exists(CONFIG['temp_folder']):
        os.makedirs(CONFIG['temp_folder'])

def get_last_processed_date(table_name, country_code):
    """Pobranie ostatniej przetworzonej daty dla danego typu danych i kraju"""
    conn = connect_to_sql()
    cursor = conn.cursor()
    
    try:
        # Sprawdzenie czy tabela istnieje
        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                SELECT MAX(DateTime) AS LastDate FROM {table_name} 
                WHERE CountryCode = ?;
            ELSE
                SELECT CAST('2020-01-01' AS DATETIME) AS LastDate;
        """, country_code)
        
        result = cursor.fetchone()
        if result and result[0]:
            # Dodaj 1 godzinę do ostatniej daty, żeby uniknąć duplikatów
            last_date = result[0] + timedelta(hours=1)
            return last_date
        
        # Jeśli brak danych, zwróć domyślną datę początkową
        return datetime(2020, 1, 1)
    except Exception as e:
        logging.error(f"Błąd podczas pobierania ostatniej daty dla {country_code}: {str(e)}")
        return datetime(2020, 1, 1)
    finally:
        conn.close()

def get_date_ranges(start_date, end_date, batch_days=60):
    """Dzieli zakres dat na mniejsze przedziały zgodne z limitami API ENTSO-E"""
    date_ranges = []
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=batch_days), end_date)
        date_ranges.append((current_start, current_end))
        current_start = current_end
    
    return date_ranges

def load_dataframe_to_sql(df, table_name, if_exists='append'):
    """Załadowanie DataFrame do tabeli SQL"""
    if df.empty:
        logging.warning(f"Brak danych do zapisania w tabeli {table_name}")
        return
    
    # Skracanie kodów krajów przed wstawieniem do bazy
    if 'CountryCode' in df.columns:
        # Funkcja konwersji długiego kodu na krótki
        def shorten_country_code(code):
            # Najpierw sprawdź, czy mamy ten kod w bidding_zones
            for zone in CONFIG['bidding_zones']:
                if zone['code'] == code:
                    return zone['short_code']
            
            # Jeśli nie znaleziono, zastosuj standardowe skrócenie
            if code.startswith('10Y'):
                # Spróbuj wyciągnąć kod kraju z nazwy (np. 10YPL-AREA-----S -> PL)
                parts = code.split('-')
                if len(parts) > 1 and len(parts[0]) >= 4:
                    country_code = parts[0][3:5]  # Wytnij 2 znaki po prefiksie 10Y
                    if country_code.isalpha():  # Sprawdź czy są to litery
                        return country_code
            
            # Jeśli nie działa, po prostu zwróć oryginalny kod
            return code
        
        # Zastosuj funkcję konwersji do kolumny CountryCode
        original_codes = df['CountryCode'].copy()
        df['CountryCode'] = df['CountryCode'].apply(shorten_country_code)
        
        # Loguj przekształcenia dla celów diagnostycznych
        if len(df) > 0:
            sample_idx = df.index[0]
            if sample_idx in original_codes.index:
                original = original_codes[sample_idx]
                shortened = df.loc[sample_idx, 'CountryCode']
                if original != shortened:
                    logging.info(f"Przekształcono kod kraju: {original} -> {shortened}")
    
    try:
        conn = connect_to_sql()
        with conn:
            cursor = conn.cursor()
            
            # Tworzenie tabeli, jeśli nie istnieje
            if table_name == 'StagingLoad':
                cursor.execute("""
                    IF OBJECT_ID('StagingLoad', 'U') IS NULL
                    CREATE TABLE StagingLoad (
                        [DateTime] DATETIME NOT NULL,
                        [CountryCode] NVARCHAR(10) NOT NULL,
                        [ActualLoad] FLOAT NULL,
                        [ForecastedLoad] FLOAT NULL,
                        PRIMARY KEY ([DateTime], [CountryCode])
                    )
                """)
            elif table_name == 'StagingGeneration':
                cursor.execute("""
                    IF OBJECT_ID('StagingGeneration', 'U') IS NULL
                    CREATE TABLE StagingGeneration (
                        [DateTime] DATETIME NOT NULL,
                        [CountryCode] NVARCHAR(10) NOT NULL,
                        [ProductionType] NVARCHAR(50) NOT NULL,
                        [Generation] FLOAT NULL,
                        PRIMARY KEY ([DateTime], [CountryCode], [ProductionType])
                    )
                """)
            elif table_name == 'StagingPrice':
                cursor.execute("""
                    IF OBJECT_ID('StagingPrice', 'U') IS NULL
                    CREATE TABLE StagingPrice (
                        [DateTime] DATETIME NOT NULL,
                        [CountryCode] NVARCHAR(10) NOT NULL,
                        [Price] FLOAT NULL,
                        PRIMARY KEY ([DateTime], [CountryCode])
                    )
                """)
            
            conn.commit()
            
            # Wstawianie danych partiami
            batch_size = 1000
            total_records = len(df)
            total_success = 0
            total_errors = 0
            
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                # Przygotowanie SQL INSERT w zależności od tabeli
                if table_name == 'StagingLoad':
                    sql = "INSERT INTO StagingLoad (DateTime, CountryCode, ActualLoad, ForecastedLoad) VALUES (?, ?, ?, ?)"
                    params = []
                    for idx, row in batch.iterrows():
                        # Konwersja DateTime na obiekt typu datetime
                        datetime_val = row['DateTime']
                        if isinstance(datetime_val, pd.Timestamp):
                            datetime_val = datetime_val.to_pydatetime()
                        
                        # Konwersja wartości liczbowych na float
                        actual_load = float(row.get('ActualLoad')) if pd.notna(row.get('ActualLoad')) else None
                        forecasted_load = float(row.get('ForecastedLoad')) if pd.notna(row.get('ForecastedLoad')) else None
                        
                        params.append((datetime_val, row['CountryCode'], actual_load, forecasted_load))
                elif table_name == 'StagingGeneration':
                    sql = "INSERT INTO StagingGeneration (DateTime, CountryCode, ProductionType, Generation) VALUES (?, ?, ?, ?)"
                    params = []
                    for idx, row in batch.iterrows():
                        # Konwersja DateTime na obiekt typu datetime
                        datetime_val = row['DateTime']
                        if isinstance(datetime_val, pd.Timestamp):
                            datetime_val = datetime_val.to_pydatetime()
                        
                        # Konwersja wartości liczbowych na float
                        generation = float(row['Generation']) if pd.notna(row['Generation']) else None
                        
                        params.append((datetime_val, row['CountryCode'], row['ProductionType'], generation))
                elif table_name == 'StagingPrice':
                    sql = "INSERT INTO StagingPrice (DateTime, CountryCode, Price) VALUES (?, ?, ?)"
                    params = []
                    for idx, row in batch.iterrows():
                        # Konwersja DateTime na obiekt typu datetime
                        datetime_val = row.name if table_name == 'StagingPrice' and isinstance(row.name, datetime) else row['DateTime']
                        if isinstance(datetime_val, pd.Timestamp):
                            datetime_val = datetime_val.to_pydatetime()
                        
                        # Konwersja wartości liczbowych na float
                        price = float(row.get('Price')) if pd.notna(row.get('Price')) else None
                        
                        params.append((datetime_val, row['CountryCode'], price))
                
                try:
                    cursor.executemany(sql, params)
                    conn.commit()
                    inserted = len(batch)
                    total_success += inserted
                    logging.info(f"Wstawiono partię {i//batch_size + 1}/{(total_records-1)//batch_size + 1} ({inserted} rekordów)")
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Błąd podczas wstawiania partii: {str(e)}")
                    
                    # Wstawianie pojedynczo w przypadku błędu
                    success_count = 0
                    error_count = 0
                    
                    for param in params:
                        try:
                            cursor.execute(sql, param)
                            conn.commit()
                            success_count += 1
                        except Exception as inner_e:
                            conn.rollback()
                            error_count += 1
                            if 'PRIMARY KEY' in str(inner_e):
                                # Duplikat klucza - to normalne zachowanie
                                pass
                            else:
                                logging.error(f"Błąd wstawiania wiersza: {str(inner_e)}")
                    
                    logging.info(f"Wstawiono {success_count} rekordów, pominięto {error_count} duplikatów")
                    total_success += success_count
                    total_errors += error_count
            
            logging.info(f"Łącznie wstawiono {total_success} rekordów, pominięto {total_errors} duplikatów")
            
            return total_success, total_errors
            
    except Exception as e:
        logging.error(f"Błąd podczas zapisywania do bazy danych: {str(e)}")
        raise

def check_sql_tables():
    """Sprawdza czy niezbędne tabele istnieją w bazie danych"""
    conn = connect_to_sql()
    cursor = conn.cursor()
    
    tables = ['StagingLoad', 'StagingGeneration', 'StagingPrice']
    missing_tables = []
    
    for table in tables:
        cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NULL SELECT 0 ELSE SELECT 1")
        exists = cursor.fetchone()[0]
        if not exists:
            missing_tables.append(table)
    
    conn.close()
    return missing_tables