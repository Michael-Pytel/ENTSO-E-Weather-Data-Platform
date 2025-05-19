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
            cursor = conn.cursor()
            
            # Specjalna obsługa dla danych generacji
            if table_name == 'StagingGeneration':
                # Przekształcenie danych z formatu szerokiego do wąskiego, jeśli to konieczne
                if 'ProductionType' not in df.columns or 'Generation' not in df.columns:
                    logging.info(f"Przekształcanie danych generacji z formatu szerokiego do wąskiego")
                    # Resetujemy indeks, aby DateTime stał się kolumną
                    reset_df = df.reset_index()
                    # Zmieniamy nazwę kolumny indeksu na 'DateTime'
                    reset_df.rename(columns={'index': 'DateTime'}, inplace=True)
                    
                    # Przekształcamy z formatu szerokiego do wąskiego
                    df = pd.melt(
                        reset_df, 
                        id_vars=['DateTime'], 
                        var_name='ProductionType', 
                        value_name='Generation'
                    )
                    
                    # Usunięcie wierszy z wartościami NaN
                    df = df.dropna(subset=['Generation'])
            else:
                # Dla pozostałych tabel - standardowa konwersja
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
            
            # Tworzenie tabeli, jeśli nie istnieje (tylko dla niestandardowej tabeli)
            if table_name != 'StagingGeneration' and table_name != 'StagingLoad' and table_name != 'StagingPrice':
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
            
            # Przygotowanie zapytania INSERT na podstawie typu tabeli
            if table_name == 'StagingGeneration':
                columns_str = "DateTime, ProductionType, Generation"
                placeholders = "?, ?, ?"
                
                # Przygotuj funkcję do przekształcania wierszy w tuplę wartości
                def row_to_values(row_idx, row_data):
                    return (row_data['DateTime'], row_data['ProductionType'], row_data['Generation'])
            else:
                # Standardowa logika dla innych tabel
                columns_str = ', '.join([f"[{col}]" for col in df.columns])
                placeholders = ', '.join(['?'] * len(df.columns))
                
                # Funkcja przekształcająca wiersz na listę wartości
                def row_to_values(row_idx, row_data):
                    return row_data.values.tolist()
            
            # Wstawianie danych partiami
            batch_size = 1000
            total_success = 0
            total_errors = 0
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Przygotowanie danych dla executemany
                batch_data = [row_to_values(idx, row) for idx, row in batch.iterrows()]
                
                try:
                    cursor.executemany(insert_sql, batch_data)
                    conn.commit()
                    total_success += len(batch)
                    print(f"Wstawiono partię {i//batch_size + 1}/{(len(df)-1)//batch_size + 1}")
                except Exception as e:
                    print(f"Błąd podczas wstawiania partii: {str(e)}")
                    # Wstawianie pojedynczo w przypadku błędu
                    success_count = 0
                    error_count = 0
                    
                    for values in batch_data:
                        try:
                            cursor.execute(insert_sql, values)
                            conn.commit()
                            success_count += 1
                        except Exception as inner_e:
                            error_count += 1
                            if 'PRIMARY KEY' in str(inner_e):
                                if table_name == 'StagingGeneration':
                                    print(f"Duplikat klucza dla DateTime: {values[0]}, ProductionType: {values[1]}")
                                else:
                                    print(f"Duplikat klucza dla DateTime: {values[0]}")
                            else:
                                print(f"Błąd wstawiania wiersza: {str(inner_e)}")
                    
                    print(f"Wstawiono {success_count} rekordów, pominięto {error_count} duplikatów")
                    total_success += success_count
                    total_errors += error_count
            
            print(f"Wstawiono {total_success} rekordów, pominięto {total_errors} duplikatów")
            print(f"Zapisano dane do tabeli {table_name}")
            
    except Exception as e:
        print(f"Błąd podczas zapisywania do bazy danych: {str(e)}")
        raise