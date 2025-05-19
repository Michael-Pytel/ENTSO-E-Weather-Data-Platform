# utils.py - Wspólne funkcje używane w skryptach
import os
import sys
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from config import CONFIG

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
        print(f"Błąd podczas łączenia z bazą danych: {str(e)}")
        sys.exit(1)

def ensure_temp_folder():
    """Zapewnienie istnienia folderu tymczasowego"""
    if not os.path.exists(CONFIG['temp_folder']):
        os.makedirs(CONFIG['temp_folder'])

def get_last_processed_date(table_name):
    """Pobranie ostatniej przetworzonej daty dla danego typu danych"""
    conn = connect_to_sql()
    cursor = conn.cursor()
    
    try:
        # Sprawdzenie czy tabela istnieje
        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                SELECT MAX(DateTime) AS LastDate FROM {table_name};
            ELSE
                SELECT CAST('2020-01-01' AS DATETIME) AS LastDate;
        """)
        
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        return datetime(2020, 1, 1)  # Domyślna data początkowa, jeśli brak danych
    except Exception as e:
        print(f"Błąd podczas pobierania ostatniej daty: {str(e)}")
        return datetime(2020, 1, 1)
    finally:
        conn.close()

def load_dataframe_to_sql(df, table_name, if_exists='replace'):
    """Załadowanie DataFrame do tabeli SQL"""
    try:
        conn = connect_to_sql()
        with conn:
            # Konwersja typu danych timestamp z indeksu do kolumny
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index().rename(columns={'index': 'DateTime'})
            
            # Sprawdzenie czy kolumna DateTime istnieje
            if 'DateTime' not in df.columns and 'time' in df.columns:
                df = df.rename(columns={'time': 'DateTime'})
            
            # Konwersja typów danych
            for col in df.columns:
                if df[col].dtype == 'float64':
                    df[col] = df[col].astype('float')
            
            # Zapisanie do SQL
            cursor = conn.cursor()
            
            # Tworzenie tabeli, jeśli nie istnieje
            columns = []
            for col in df.columns:
                dtype = df[col].dtype
                if col == 'DateTime':
                    columns.append(f"[{col}] DATETIME")
                elif 'float' in str(dtype):
                    columns.append(f"[{col}] FLOAT")
                elif 'int' in str(dtype):
                    columns.append(f"[{col}] INT")
                else:
                    columns.append(f"[{col}] NVARCHAR(255)")
            
            create_table_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_table_sql)
            conn.commit()
            
            # Wstawianie danych
            if if_exists == 'replace':
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                conn.commit()
            
            # Przygotowanie parametrów dla zapytania INSERT
            placeholders = ', '.join(['?'] * len(df.columns))
            columns_str = ', '.join([f"[{col}]" for col in df.columns])
            
            # Wstawianie danych partiami
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                cursor.executemany(insert_sql, batch.values.tolist())
                conn.commit()
                
            print(f"Zapisano {len(df)} wierszy do tabeli {table_name}")
    except Exception as e:
        print(f"Błąd podczas zapisywania do bazy danych: {str(e)}")
        raise