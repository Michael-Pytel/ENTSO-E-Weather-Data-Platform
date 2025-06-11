""""
DataCleaner.py
Czyszczenie i ładowanie danych do hurtowni danych
Zapewnia poprawne ładowanie danych bez wartości NULL
Obsługuje powiązania między faktami a wymiarami z defaultowymi wartościami
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import pyodbc
import logging
import traceback
import gc
import os
import sys
from typing import Dict, List, Optional, Tuple, Any

class DataCleaner:
    """Klasa czyszcząca i ładująca dane do hurtowni"""
    
    def __init__(self, connection_string: str):
        """
        Inicjalizacja czyszczenia danych
        
        Args:
            connection_string: String połączenia z bazą danych
        """
        self.connection_string = connection_string
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Mapowanie tabel staging do tabel docelowych
        self.staging_to_target_tables = {
            # Tabele wymiarów
            'staging_dim_date': 'dim_date',
            'staging_dim_time': 'dim_time',
            'staging_dim_bidding_zone': 'dim_bidding_zone',
            'staging_dim_weather_zone': 'dim_weather_zone',
            'staging_dim_generation_type': 'dim_generation_type',
            'staging_dim_weather_condition': 'dim_weather_condition',
            'staging_dim_socioeconomic_profile': 'dim_socioeconomic_profile',
            
            # Tabele faktów
            'staging_fact_energy_weather': 'fact_energy_weather',
            
            # Tabele źródłowe
            'staging_entso_actual_load': 'src_entso_actual_load',
            'staging_entso_generation': 'src_entso_generation',
            'staging_entso_forecast': 'src_entso_forecast',
            'staging_weather_data': 'src_weather_data',
            'staging_climate_data': 'src_climate_data',
            'staging_eurostat_integrated': 'src_eurostat_integrated'
        }
        
        # Mapowanie kolumn ID dla wymiarów
        self.dimension_id_columns = {
            'dim_date': 'date_id',
            'dim_time': 'time_id',
            'dim_bidding_zone': 'bidding_zone_id',
            'dim_weather_zone': 'weather_zone_id',
            'dim_generation_type': 'generation_type_id',
            'dim_weather_condition': 'weather_condition_id',
            'dim_socioeconomic_profile': 'socioeconomic_profile_id'
        }
        
        # Domyślne wartości dla nieznanych wartości w wymiarach
        self.default_dimension_values = {
            'dim_date': {
                'date_id': 0,
                'full_date': date(1900, 1, 1),
                'day_of_week': 'Unknown',
                'day_of_month': 0,
                'month': 0,
                'month_name': 'Unknown',
                'quarter': 0,
                'year': 0,
                'season': 'Unknown',
                'is_holiday': 'No',
                'holiday_name': 'None',
                'holiday_type': 'None',
                'is_school_day': 'No',
                'is_weekend': 'No'
            },
            'dim_time': {
                'time_id': 0,
                'hour': 0,
                'minute': 0,
                'day_period': 'Unknown',
                'is_peak_hour': 'No'
            },
            'dim_bidding_zone': {
                'bidding_zone_id': 0,
                'bidding_zone_code': 'UNKNOWN',
                'bidding_zone_name': 'Unknown',
                'primary_country': 'UNK',
                'secondary_countries': 'None',
                'control_area': 'Unknown',
                'timezone': 'UTC',
                'population': 0,
                'gdp_per_capita': 0.0,
                'energy_intensity': 0.0,
                'electricity_price_avg': 0.0,
                'valid_from': date(1900, 1, 1),
                'valid_to': date(9999, 12, 31),
                'is_current': 'Yes',
                'year': 0
            },
            'dim_weather_zone': {
                'weather_zone_id': 0,
                'weather_zone_name': 'Unknown',
                'bidding_zone_id': 0,
                'climate_zone': 'Unknown',
                'elevation_avg': 0.0,
                'coastal_proximity': 'Unknown',
                'urbanization_level': 'Unknown',
                'valid_from': date(1900, 1, 1),
                'valid_to': date(9999, 12, 31),
                'is_current': 'Yes'
            },
            'dim_generation_type': {
                'generation_type_id': 0,
                'entso_code': 'B20',
                'generation_category': 'Unknown',
                'generation_type': 'Unknown',
                'is_intermittent': 'No',
                'fuel_source': 'Unknown'
            },
            'dim_weather_condition': {
                'weather_condition_id': 0,
                'condition_type': 'Unknown',
                'condition_severity': 'None',
                'is_extreme_weather': 'No',
                'extreme_weather_type': 'None'
            },
            'dim_socioeconomic_profile': {
                'socioeconomic_profile_id': 0,
                'bidding_zone_code': 'UNKNOWN',
                'country_code': 'UNK',
                'country_name': 'Unknown',
                'year': 0,
                'avg_income_level': 0.0,
                'unemployment_rate': 0.0,
                'urbanization_rate': 0.0,
                'service_sector_percentage': 0.0,
                'industry_sector_percentage': 0.0,
                'energy_poverty_rate': 0.0,
                'residential_percentage': 0.0,
                'commercial_percentage': 0.0,
                'industrial_percentage': 0.0,
                'avg_household_size': 0.0,
                'primary_heating_type': 'Unknown',
                'population': 0,
                'valid_from': date(1900, 1, 1),
                'valid_to': date(9999, 12, 31),
                'is_current': 'Yes'
            }
        }
    
    def check_staging_tables(self) -> Dict[str, int]:
        """
        Sprawdzenie, które tabele staging zawierają dane
        
        Returns:
            Słownik z liczbą rekordów w każdej tabeli staging
        """
        self.logger.info("Checking staging tables")
        results = {}
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for staging_table in self.staging_to_target_tables.keys():
                try:
                    cursor.execute(f"""
                        IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                            SELECT COUNT(*) FROM {staging_table}
                        ELSE
                            SELECT -1
                    """)
                    
                    row_count = cursor.fetchone()[0]
                    
                    if row_count >= 0:
                        self.logger.info(f"Table {staging_table} exists with {row_count} records")
                        results[staging_table] = row_count
                    else:
                        self.logger.warning(f"Table {staging_table} does not exist")
                        results[staging_table] = 0
                    
                except Exception as e:
                    self.logger.error(f"Error checking table {staging_table}: {str(e)}")
                    results[staging_table] = 0
            
            conn.close()
            return results
            
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            return {}
    
    def ensure_target_tables_exist(self) -> bool:
        """
        Upewnienie się, że tabele docelowe istnieją i mają właściwą strukturę
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Ensuring target tables exist")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Upewnij się, że tabele wymiarów istnieją
            dimensions = [
                'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
                'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile'
            ]
            
            for dim in dimensions:
                staging_table = f"staging_{dim}"
                
                # Sprawdź czy tabela docelowa istnieje
                cursor.execute(f"""
                    IF OBJECT_ID('{dim}', 'U') IS NOT NULL
                        SELECT 1
                    ELSE
                        SELECT 0
                """)
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    self.logger.info(f"Creating table {dim} from staging")
                    
                    # Sprawdź czy tabela staging istnieje
                    cursor.execute(f"""
                        IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                            SELECT 1
                        ELSE
                            SELECT 0
                    """)
                    
                    staging_exists = cursor.fetchone()[0]
                    
                    if not staging_exists:
                        self.logger.error(f"Staging table {staging_table} does not exist")
                        self._create_default_dimension_table(conn, dim)
                    else:
                        # Utwórz tabelę docelową na podstawie stagingu
                        self._create_target_from_staging(conn, staging_table, dim)
                else:
                    self.logger.info(f"Table {dim} already exists")
                    
                    # Sprawdź czy tabela ma rekord z ID=0 (domyślny)
                    self._ensure_default_dimension_record(conn, dim)
            
            # Upewnij się, że tabela faktów istnieje
            cursor.execute("""
                IF OBJECT_ID('fact_energy_weather', 'U') IS NOT NULL
                    SELECT 1
                ELSE
                    SELECT 0
            """)
            
            fact_table_exists = cursor.fetchone()[0]
            
            if not fact_table_exists:
                self.logger.info("Creating fact table")
                # Sprawdź czy tabela staging istnieje
                cursor.execute("""
                    IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL
                        SELECT 1
                    ELSE
                        SELECT 0
                """)
                
                staging_fact_exists = cursor.fetchone()[0]
                
                if not staging_fact_exists:
                    self.logger.error("Staging fact table does not exist")
                    self._create_default_fact_table(conn)
                else:
                    # Utwórz tabelę faktów na podstawie stagingu
                    self._create_target_from_staging(conn, 'staging_fact_energy_weather', 'fact_energy_weather')
            else:
                self.logger.info("Fact table already exists")
            
            # Upewnij się, że tabele źródłowe istnieją
            source_tables = [
                'src_entso_actual_load', 'src_entso_generation', 'src_entso_forecast',
                'src_weather_data', 'src_climate_data', 'src_eurostat_integrated'
            ]
            
            for src_table in source_tables:
                staging_table = f"staging_{src_table.replace('src_', '')}"
                
                cursor.execute(f"""
                    IF OBJECT_ID('{src_table}', 'U') IS NOT NULL
                        SELECT 1
                    ELSE
                        SELECT 0
                """)
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    self.logger.info(f"Creating source table {src_table} from staging")
                    
                    cursor.execute(f"""
                        IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                            SELECT 1
                        ELSE
                            SELECT 0
                    """)
                    
                    staging_exists = cursor.fetchone()[0]
                    
                    if not staging_exists:
                        self.logger.warning(f"Staging table {staging_table} does not exist, skipping {src_table}")
                    else:
                        # Utwórz tabelę źródłową na podstawie stagingu
                        self._create_target_from_staging(conn, staging_table, src_table)
                else:
                    self.logger.info(f"Source table {src_table} already exists")
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error ensuring target tables: {str(e)}")
            return False
    
    def _create_target_from_staging(self, conn, staging_table: str, target_table: str):
        """
        Tworzenie tabeli docelowej na podstawie tabeli staging
        
        Args:
            conn: Połączenie z bazą danych
            staging_table: Nazwa tabeli staging
            target_table: Nazwa tabeli docelowej
        """
        try:
            cursor = conn.cursor()
            
            # Pobierz strukturę tabeli staging
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
                       NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{staging_table}'
                ORDER BY ORDINAL_POSITION
            """)
            
            columns = cursor.fetchall()
            
            if not columns:
                self.logger.error(f"No columns found for {staging_table}")
                return
            
            # Przygotuj definicję kolumn
            column_defs = []
            primary_key = None
            
            # Jeśli to tabela wymiarów, użyj odpowiedniego id
            if target_table.startswith('dim_'):
                id_col = self.dimension_id_columns.get(target_table, f"{target_table.replace('dim_', '')}_id")
                primary_key = id_col
                column_defs.append(f"{primary_key} BIGINT IDENTITY(1,1) PRIMARY KEY")
            # Jeśli to tabela faktów, użyj energy_weather_id
            elif target_table == 'fact_energy_weather':
                primary_key = 'energy_weather_id'
                column_defs.append(f"{primary_key} BIGINT IDENTITY(1,1) PRIMARY KEY")
            # Dla tabel źródłowych użyj standardowego formatu
            else:
                primary_key = f"{target_table.replace('src_', '')}_id"
                column_defs.append(f"{primary_key} BIGINT IDENTITY(1,1) PRIMARY KEY")
            
            for col in columns:
                col_name = col.COLUMN_NAME
                data_type = col.DATA_TYPE
                max_length = col.CHARACTER_MAXIMUM_LENGTH
                precision = col.NUMERIC_PRECISION
                scale = col.NUMERIC_SCALE
                
                # Pomiń kolumnę id z tabeli staging jeśli nie jest to pierwszy przypadek
                if col_name.lower() == 'id' and primary_key is not None:
                    continue
                else:
                    # Określ definicję typu danych
                    type_def = data_type
                    if data_type in ['varchar', 'nvarchar', 'char', 'nchar']:
                        if max_length == -1:
                            type_def = f"{data_type}(MAX)"
                        else:
                            type_def = f"{data_type}({max_length})"
                    elif data_type in ['decimal', 'numeric']:
                        type_def = f"{data_type}({precision},{scale})"
                    
                    column_defs.append(f"{col_name} {type_def}")
            
            # Dodaj kolumnę created_at jeśli nie istnieje
            if not any(col.COLUMN_NAME.lower() == 'created_at' for col in columns):
                column_defs.append("created_at DATETIME2 DEFAULT GETDATE()")
            
            # Utwórz tabelę docelową
            create_sql = f"""
            CREATE TABLE {target_table} (
                {', '.join(column_defs)}
            )
            """
            
            self.logger.info(f"Creating table {target_table}")
            cursor.execute(create_sql)
            conn.commit()
            
            self.logger.info(f"Table {target_table} created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating target table {target_table}: {str(e)}")
            conn.rollback()
    
    def _create_default_dimension_table(self, conn, dim_table: str):
        """
        Tworzenie domyślnej tabeli wymiarów, gdy tabela staging nie istnieje
        
        Args:
            conn: Połączenie z bazą danych
            dim_table: Nazwa tabeli wymiarów
        """
        try:
            cursor = conn.cursor()
            
            # Pobierz domyślne wartości i strukture
            default_values = self.default_dimension_values.get(dim_table, {})
            
            if not default_values:
                self.logger.error(f"No default values defined for {dim_table}")
                return
            
            # Ustaw nazwę kolumny ID na podstawie rzeczywistej konwencji nazewnictwa
            id_column = f"{dim_table.replace('dim_', '')}_id"  # np. date_id, time_id
            
            # Przygotuj definicję kolumn
            column_defs = []
            insert_columns = []
            insert_values = []
            
            # Dodaj klucz główny
            column_defs.append(f"{id_column} BIGINT IDENTITY(0,1) PRIMARY KEY")
            
            for col_name, default_value in default_values.items():
                if col_name == f"{dim_table}_id" or col_name == id_column:
                    continue  # Pomiń kolumnę klucza głównego
                
                # Określ typ danych na podstawie domyślnej wartości
                if isinstance(default_value, str):
                    if len(default_value) > 100:
                        col_type = "NVARCHAR(MAX)"
                    else:
                        col_type = f"NVARCHAR({max(len(default_value) * 2, 50)})"
                elif isinstance(default_value, int):
                    col_type = "INT"
                elif isinstance(default_value, float):
                    col_type = "DECIMAL(15,2)"
                elif isinstance(default_value, bool):
                    col_type = "BIT"
                elif isinstance(default_value, (date, datetime)):
                    col_type = "DATE"
                else:
                    col_type = "NVARCHAR(100)"
                
                column_defs.append(f"{col_name} {col_type}")
                insert_columns.append(col_name)
                
                # Przygotuj wartości do wstawienia
                if isinstance(default_value, str):
                    insert_values.append(f"'{default_value}'")
                elif isinstance(default_value, (int, float)):
                    insert_values.append(str(default_value))
                elif isinstance(default_value, bool):
                    insert_values.append('1' if default_value else '0')
                elif isinstance(default_value, (date, datetime)):
                    insert_values.append(f"'{default_value.isoformat()}'")
                else:
                    insert_values.append("NULL")
            
            # Dodaj kolumnę created_at
            column_defs.append("created_at DATETIME2 DEFAULT GETDATE()")
            
            # Utwórz tabelę
            create_sql = f"""
            CREATE TABLE {dim_table} (
                {', '.join(column_defs)}
            )
            """
            
            self.logger.info(f"Creating default dimension table {dim_table}")
            cursor.execute(create_sql)
            
            # Wstaw domyślny rekord z ID=0
            if insert_columns and insert_values:
                insert_sql = f"""
                SET IDENTITY_INSERT {dim_table} ON;
                INSERT INTO {dim_table} ({id_column}, {', '.join(insert_columns)})
                VALUES (0, {', '.join(insert_values)});
                SET IDENTITY_INSERT {dim_table} OFF;
                """
                
                self.logger.info(f"Inserting default record to {dim_table}")
                cursor.execute(insert_sql)
            
            conn.commit()
            self.logger.info(f"Default dimension table {dim_table} created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating default dimension table {dim_table}: {str(e)}")
            conn.rollback()
    
    def _create_default_fact_table(self, conn):
        """
        Tworzenie domyślnej tabeli faktów, gdy tabela staging nie istnieje
        
        Args:
            conn: Połączenie z bazą danych
        """
        try:
            cursor = conn.cursor()
            
            # Standardowa struktura tabeli faktów
            create_sql = """
            CREATE TABLE fact_energy_weather (
                fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                date_id INT NOT NULL,
                time_id INT NOT NULL,
                bidding_zone_id INT NOT NULL,
                weather_zone_id INT NOT NULL,
                generation_type_id INT,
                weather_condition_id INT,
                socioeconomic_profile_id INT,
                actual_consumption DECIMAL(15,2),
                forecasted_consumption DECIMAL(15,2),
                consumption_deviation DECIMAL(10,2),
                generation_amount DECIMAL(15,2),
                capacity_factor DECIMAL(5,2),
                renewable_percentage DECIMAL(5,2),
                per_capita_consumption DECIMAL(10,5),
                temperature_avg DECIMAL(5,2),
                temperature_min DECIMAL(5,2),
                temperature_max DECIMAL(5,2),
                humidity DECIMAL(5,2),
                precipitation DECIMAL(8,2),
                wind_speed DECIMAL(5,2),
                wind_direction INT,
                cloud_cover DECIMAL(5,2),
                solar_radiation DECIMAL(8,2),
                air_pressure DECIMAL(8,2),
                heating_degree_days DECIMAL(5,2),
                cooling_degree_days DECIMAL(5,2),
                created_at DATETIME2 DEFAULT GETDATE(),
                CONSTRAINT FK_fact_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
                CONSTRAINT FK_fact_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
                CONSTRAINT FK_fact_bidding_zone FOREIGN KEY (bidding_zone_id) REFERENCES dim_bidding_zone(bidding_zone_id),
                CONSTRAINT FK_fact_weather_zone FOREIGN KEY (weather_zone_id) REFERENCES dim_weather_zone(weather_zone_id),
                CONSTRAINT FK_fact_generation_type FOREIGN KEY (generation_type_id) REFERENCES dim_generation_type(generation_type_id),
                CONSTRAINT FK_fact_weather_condition FOREIGN KEY (weather_condition_id) REFERENCES dim_weather_condition(weather_condition_id),
                CONSTRAINT FK_fact_socioeconomic_profile FOREIGN KEY (socioeconomic_profile_id) REFERENCES dim_socioeconomic_profile(socioeconomic_profile_id)
            )
            """
            
            self.logger.info("Creating default fact_energy_weather table")
            cursor.execute(create_sql)
            conn.commit()
            
            self.logger.info("Default fact table created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating default fact table: {str(e)}")
            conn.rollback()
    
    def _ensure_default_dimension_record(self, conn, dim_table: str):
        """
        Upewnienie się, że tabela wymiarów ma rekord z ID=0 (domyślny/nieznany)
        
        Args:
            conn: Połączenie z bazą danych
            dim_table: Nazwa tabeli wymiarów
        """
        try:
            cursor = conn.cursor()
            
            # Znajdź nazwę kolumny ID
            id_column = self._get_dimension_id_column(conn, dim_table)
            
            # Sprawdź czy rekord z ID=0 istnieje
            cursor.execute(f"""
                IF EXISTS (SELECT 1 FROM {dim_table} WHERE {id_column} = 0)
                    SELECT 1
                ELSE
                    SELECT 0
            """)
            
            exists = cursor.fetchone()[0]
            
            if not exists:
                self.logger.info(f"Adding default record to {dim_table}")
                
                # Pobierz strukturę tabeli
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{dim_table}'
                    AND COLUMN_NAME <> '{id_column}'
                    AND COLUMN_NAME <> 'created_at'
                    ORDER BY ORDINAL_POSITION
                """)
                
                columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                # Pobierz domyślne wartości
                default_values = self.default_dimension_values.get(dim_table, {})
                
                if not default_values:
                    self.logger.error(f"No default values defined for {dim_table}")
                    return
                
                # Przygotuj wartości do wstawienia
                values = []
                for col in columns:
                    default_value = default_values.get(col)
                    
                    if default_value is None:
                        values.append("NULL")
                    elif isinstance(default_value, str):
                        values.append(f"'{default_value}'")
                    elif isinstance(default_value, (int, float)):
                        values.append(str(default_value))
                    elif isinstance(default_value, bool):
                        values.append('1' if default_value else '0')
                    elif isinstance(default_value, (date, datetime)):
                        values.append(f"'{default_value.isoformat()}'")
                    else:
                        values.append("NULL")
                
                # Wstaw domyślny rekord z ID=0
                insert_sql = f"""
                SET IDENTITY_INSERT {dim_table} ON;
                INSERT INTO {dim_table} ({id_column}, {', '.join(columns)})
                VALUES (0, {', '.join(values)});
                SET IDENTITY_INSERT {dim_table} OFF;
                """
                
                self.logger.info(f"Inserting default record to {dim_table}")
                cursor.execute(insert_sql)
                conn.commit()
                
                self.logger.info(f"Default record added to {dim_table}")
            else:
                self.logger.info(f"Default record already exists in {dim_table}")
            
        except Exception as e:
            self.logger.error(f"Error ensuring default record in {dim_table}: {str(e)}")
            conn.rollback()
    
    def load_dimensions_from_staging(self) -> bool:
        """
        Ładowanie danych wymiarów z tabel staging do tabel docelowych
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Loading dimensions from staging")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            dimensions = [
                'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
                'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile'
            ]
            
            for dim in dimensions:
                staging_table = f"staging_{dim}"
                
                # Sprawdź czy tabela staging istnieje i ma dane
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                        SELECT COUNT(*) FROM {staging_table}
                    ELSE
                        SELECT 0
                """)
                
                staging_count = cursor.fetchone()[0]
                
                if staging_count > 0:
                    self.logger.info(f"Loading {staging_count} records from {staging_table} to {dim}")
                    
                    # Znajdź nazwę kolumny ID dla tabeli docelowej
                    id_column = self._get_dimension_id_column(conn, dim)
                    
                    # Pobierz kolumny tabeli docelowej
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{dim}'
                        AND COLUMN_NAME <> '{id_column}'
                        AND COLUMN_NAME <> 'created_at'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    target_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Pobierz kolumny tabeli staging
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{staging_table}'
                        AND COLUMN_NAME <> 'id'
                        AND COLUMN_NAME <> 'created_at'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    staging_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Znajdź wspólne kolumny
                    common_columns = [col for col in staging_columns if col in target_columns]
                    
                    if not common_columns:
                        self.logger.error(f"No matching columns between {staging_table} and {dim}")
                        continue
                    
                    # Podejście uproszczone - usuń wszystkie rekordy z ID>0 i wstaw ponownie
                    # Najpierw sprawdź max ID w tabeli docelowej (dla bezpieczeństwa)
                    cursor.execute(f"""
                        SELECT ISNULL(MAX({id_column}), 0) FROM {dim}
                    """)
                    max_id = cursor.fetchone()[0]
                    
                    # Teraz usuń wszystkie rekordy oprócz ID=0
                    self.logger.info(f"Removing existing records from {dim} (keeping default record with ID=0)")
                    cursor.execute(f"""
                        DELETE FROM {dim} WHERE {id_column} > 0
                    """)
                    conn.commit()
                    
                    # Wstaw nowe dane
                    self.logger.info(f"Inserting {staging_count} records into {dim}")
                    
                    # Użyj normalnego INSERT bez sprawdzania unikalności
                    insert_sql = f"""
                    INSERT INTO {dim} ({', '.join(common_columns)})
                    SELECT {', '.join(common_columns)}
                    FROM {staging_table}
                    """
                    
                    cursor.execute(insert_sql)
                    conn.commit()
                    
                    # Sprawdź liczbę wstawionych rekordów
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM {dim} WHERE {id_column} > 0
                    """)
                    inserted_count = cursor.fetchone()[0]
                    
                    self.logger.info(f"Successfully inserted {inserted_count} records into {dim}")
                else:
                    self.logger.warning(f"No data in {staging_table}, using default values for {dim}")
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading dimensions: {str(e)}")
            return False
    
    def load_facts_from_staging(self) -> bool:
        """
        Ładowanie danych faktów z tabeli staging do tabeli docelowej
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Loading facts from staging")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Sprawdź czy tabela staging istnieje i ma dane
            cursor.execute("""
                IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL
                    SELECT COUNT(*) FROM staging_fact_energy_weather
                ELSE
                    SELECT 0
            """)
            
            staging_count = cursor.fetchone()[0]
            
            if staging_count > 0:
                self.logger.info(f"Loading {staging_count} fact records from staging")
                
                # Pobierz kolumny tabeli docelowej
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'fact_energy_weather'
                    AND COLUMN_NAME <> 'energy_weather_id'
                    AND COLUMN_NAME <> 'fact_id'
                    AND COLUMN_NAME <> 'created_at'
                    ORDER BY ORDINAL_POSITION
                """)
                
                target_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                # Pobierz kolumny tabeli staging
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'staging_fact_energy_weather'
                    AND COLUMN_NAME <> 'id'
                    AND COLUMN_NAME <> 'created_at'
                    ORDER BY ORDINAL_POSITION
                """)
                
                staging_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                # Znajdź wspólne kolumny
                common_columns = [col for col in staging_columns if col in target_columns]
                
                if not common_columns:
                    self.logger.error("No matching columns between staging_fact_energy_weather and fact_energy_weather")
                    return False
                
                # Najpierw czyszczenie tabeli faktów
                self.logger.info("Cleaning fact_energy_weather table")
                cursor.execute("TRUNCATE TABLE fact_energy_weather")
                
                # Zliczanie wartości wymiarów z ID=0
                self.logger.info("Checking for NULL dimension IDs in staging data")
                for dim_column in ['date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id',
                                  'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id']:
                    if dim_column in common_columns:
                        cursor.execute(f"""
                            SELECT COUNT(*) FROM staging_fact_energy_weather
                            WHERE {dim_column} = 0 OR {dim_column} IS NULL
                        """)
                        null_count = cursor.fetchone()[0]
                        if null_count > 0:
                            self.logger.warning(f"Found {null_count} records with NULL/0 {dim_column}")
                
                # Upewnij się, że wymiary referencyjne mają domyślne wartości
                self._ensure_dimension_defaults(conn)
                
                # Wstaw dane ze stagingu, zamieniając NULL na 0 dla kluczy obcych
                insert_columns = []
                select_columns = []
                
                for col in common_columns:
                    insert_columns.append(col)
                    
                    # Dla kluczy obcych zastąp NULL wartością 0
                    if col.endswith('_id'):
                        select_columns.append(f"ISNULL({col}, 0) AS {col}")
                    else:
                        select_columns.append(col)
                
                insert_sql = f"""
                INSERT INTO fact_energy_weather ({', '.join(insert_columns)})
                SELECT {', '.join(select_columns)}
                FROM staging_fact_energy_weather
                """
                
                cursor.execute(insert_sql)
                conn.commit()
                
                # Sprawdź liczbę wstawionych rekordów
                cursor.execute("SELECT COUNT(*) FROM fact_energy_weather")
                inserted_count = cursor.fetchone()[0]
                
                self.logger.info(f"Inserted {inserted_count} records into fact_energy_weather")
                
                return True
            else:
                self.logger.warning("No fact data in staging, skipping")
                return False
            
        except Exception as e:
            self.logger.error(f"Error loading facts: {str(e)}")
            return False
    
    def _ensure_dimension_defaults(self, conn):
        """
        Upewnienie się, że wszystkie wymiary mają domyślne rekordy z ID=0
        
        Args:
            conn: Połączenie z bazą danych
        """
        dimensions = [
            'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
            'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile'
        ]
        
        for dim in dimensions:
            self._ensure_default_dimension_record(conn, dim)
    
    def clean_and_load_source_tables(self) -> bool:
        """
        Czyszczenie i ładowanie danych źródłowych z tabel staging
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Cleaning and loading source tables")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            source_mapping = {
                'staging_entso_actual_load': 'src_entso_actual_load',
                'staging_entso_generation': 'src_entso_generation',
                'staging_entso_forecast': 'src_entso_forecast',
                'staging_weather_data': 'src_weather_data',
                'staging_climate_data': 'src_climate_data',
                'staging_eurostat_integrated': 'src_eurostat_integrated'
            }
            
            for staging_table, target_table in source_mapping.items():
                # Sprawdź czy tabela staging istnieje i ma dane
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                        SELECT COUNT(*) FROM {staging_table}
                    ELSE
                        SELECT 0
                """)
                
                staging_count = cursor.fetchone()[0]
                
                if staging_count > 0:
                    self.logger.info(f"Loading {staging_count} records from {staging_table} to {target_table}")
                    
                    # Sprawdź czy tabela docelowa istnieje
                    cursor.execute(f"""
                        IF OBJECT_ID('{target_table}', 'U') IS NOT NULL
                            SELECT 1
                        ELSE
                            SELECT 0
                    """)
                    
                    table_exists = cursor.fetchone()[0]
                    
                    if not table_exists:
                        self.logger.info(f"Creating table {target_table} from {staging_table}")
                        self._create_target_from_staging(conn, staging_table, target_table)
                    
                    # Pobierz kolumny tabeli docelowej
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{target_table}'
                        AND COLUMN_NAME <> '{target_table.replace("src_", "")}_id'
                        AND COLUMN_NAME <> 'created_at'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    target_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Pobierz kolumny tabeli staging
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{staging_table}'
                        AND COLUMN_NAME <> 'id'
                        AND COLUMN_NAME <> 'created_at'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    staging_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Znajdź wspólne kolumny
                    common_columns = [col for col in staging_columns if col in target_columns]
                    
                    if not common_columns:
                        self.logger.error(f"No matching columns between {staging_table} and {target_table}")
                        continue
                    
                    # Czyszczenie tabeli docelowej
                    self.logger.info(f"Cleaning {target_table}")
                    cursor.execute(f"TRUNCATE TABLE {target_table}")
                    
                    # Wstaw dane ze stagingu
                    insert_sql = f"""
                    INSERT INTO {target_table} ({', '.join(common_columns)})
                    SELECT {', '.join(common_columns)}
                    FROM {staging_table}
                    """
                    
                    cursor.execute(insert_sql)
                    conn.commit()
                    
                    # Sprawdź liczbę wstawionych rekordów
                    cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
                    inserted_count = cursor.fetchone()[0]
                    
                    self.logger.info(f"Inserted {inserted_count} records into {target_table}")
                else:
                    self.logger.warning(f"No data in {staging_table}, skipping {target_table}")
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading source tables: {str(e)}")
            return False
    
    def validate_data_relationships(self) -> Dict[str, int]:
        """
        Walidacja relacji pomiędzy faktami a wymiarami
        
        Returns:
            Słownik z liczbą potencjalnych problemów
        """
        self.logger.info("Validating data relationships")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Sprawdź czy wszystkie klucze obce w tabeli faktów są poprawne
            validation_results = {}
            foreign_keys = [
                ('date_id', 'dim_date'),
                ('time_id', 'dim_time'),
                ('bidding_zone_id', 'dim_bidding_zone'),
                ('weather_zone_id', 'dim_weather_zone'),
                ('generation_type_id', 'dim_generation_type'),
                ('weather_condition_id', 'dim_weather_condition'),
                ('socioeconomic_profile_id', 'dim_socioeconomic_profile')
            ]
            
            for fk_column, dim_table in foreign_keys:
                # Znajdź nazwę kolumny ID dla tabeli wymiarów
                dim_id_column = self._get_dimension_id_column(conn, dim_table)
                
                # Sprawdź liczbę rekordów faktów z nieistniejącymi powiązaniami
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM fact_energy_weather f
                    LEFT JOIN {dim_table} d ON f.{fk_column} = d.{dim_id_column}
                    WHERE d.{dim_id_column} IS NULL AND f.{fk_column} IS NOT NULL
                """)
                
                invalid_count = cursor.fetchone()[0]
                
                if invalid_count > 0:
                    self.logger.warning(f"Found {invalid_count} fact records with invalid {fk_column}")
                    validation_results[fk_column] = invalid_count
                else:
                    self.logger.info(f"All {fk_column} references are valid")
                    validation_results[fk_column] = 0
            
            # Sprawdź liczbę NULL w wymiarach obowiązkowych
            required_dims = ['date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id']
            
            for dim in required_dims:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM fact_energy_weather 
                    WHERE {dim} IS NULL OR {dim} = 0
                """)
                
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    self.logger.warning(f"Found {null_count} fact records with NULL/0 {dim}")
                    validation_results[f"{dim}_null"] = null_count
                else:
                    self.logger.info(f"No NULL {dim} values found")
                    validation_results[f"{dim}_null"] = 0
            
            # Sprawdź, czy temperatura jest w rozsądnym zakresie
            cursor.execute("""
                SELECT COUNT(*) 
                FROM fact_energy_weather 
                WHERE temperature_avg < -80 OR temperature_avg > 80
            """)
            
            invalid_temp = cursor.fetchone()[0]
            
            if invalid_temp > 0:
                self.logger.warning(f"Found {invalid_temp} fact records with extreme temperature values")
                validation_results['extreme_temperature'] = invalid_temp
            else:
                self.logger.info("All temperature values are within reasonable range")
                validation_results['extreme_temperature'] = 0
            
            conn.close()
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating data relationships: {str(e)}")
            return {'error': 1}
    def validate_data_relationships(self) -> Dict[str, int]:
        """
        Walidacja relacji pomiędzy faktami a wymiarami
        
        Returns:
            Słownik z liczbą potencjalnych problemów
        """
        self.logger.info("Validating data relationships")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Sprawdź czy wszystkie klucze obce w tabeli faktów są poprawne
            validation_results = {}
            foreign_keys = [
                ('date_id', 'dim_date'),
                ('time_id', 'dim_time'),
                ('bidding_zone_id', 'dim_bidding_zone'),
                ('weather_zone_id', 'dim_weather_zone'),
                ('generation_type_id', 'dim_generation_type'),
                ('weather_condition_id', 'dim_weather_condition'),
                ('socioeconomic_profile_id', 'dim_socioeconomic_profile')
            ]
            
            for fk_column, dim_table in foreign_keys:
                # Sprawdź liczbę rekordów faktów z nieistniejącymi powiązaniami
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM fact_energy_weather f
                    LEFT JOIN {dim_table} d ON f.{fk_column} = d.{dim_table}_id
                    WHERE d.{dim_table}_id IS NULL AND f.{fk_column} IS NOT NULL
                """)
                
                invalid_count = cursor.fetchone()[0]
                
                if invalid_count > 0:
                    self.logger.warning(f"Found {invalid_count} fact records with invalid {fk_column}")
                    validation_results[fk_column] = invalid_count
                else:
                    self.logger.info(f"All {fk_column} references are valid")
                    validation_results[fk_column] = 0
            
            # Sprawdź liczbę NULL w wymiarach obowiązkowych
            required_dims = ['date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id']
            
            for dim in required_dims:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM fact_energy_weather 
                    WHERE {dim} IS NULL OR {dim} = 0
                """)
                
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    self.logger.warning(f"Found {null_count} fact records with NULL/0 {dim}")
                    validation_results[f"{dim}_null"] = null_count
                else:
                    self.logger.info(f"No NULL {dim} values found")
                    validation_results[f"{dim}_null"] = 0
            
            # Sprawdź, czy temperatura jest w rozsądnym zakresie
            cursor.execute("""
                SELECT COUNT(*) 
                FROM fact_energy_weather 
                WHERE temperature_avg < -80 OR temperature_avg > 80
            """)
            
            invalid_temp = cursor.fetchone()[0]
            
            if invalid_temp > 0:
                self.logger.warning(f"Found {invalid_temp} fact records with extreme temperature values")
                validation_results['extreme_temperature'] = invalid_temp
            else:
                self.logger.info("All temperature values are within reasonable range")
                validation_results['extreme_temperature'] = 0
            
            conn.close()
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating data relationships: {str(e)}")
            return {'error': 1}
    
    def fix_data_issues(self) -> bool:
        """
        Naprawianie potencjalnych problemów z danymi
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Fixing data issues")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Napraw nieistniejące klucze obce, ustawiając je na 0 (domyślna wartość)
            foreign_keys = [
                ('date_id', 'dim_date'),
                ('time_id', 'dim_time'),
                ('bidding_zone_id', 'dim_bidding_zone'),
                ('weather_zone_id', 'dim_weather_zone'),
                ('generation_type_id', 'dim_generation_type'),
                ('weather_condition_id', 'dim_weather_condition'),
                ('socioeconomic_profile_id', 'dim_socioeconomic_profile')
            ]
            
            for fk_column, dim_table in foreign_keys:
                # Znajdź nazwę kolumny ID dla tabeli wymiarów
                dim_id_column = self._get_dimension_id_column(conn, dim_table)
                
                cursor.execute(f"""
                    UPDATE f
                    SET f.{fk_column} = 0
                    FROM fact_energy_weather f
                    LEFT JOIN {dim_table} d ON f.{fk_column} = d.{dim_id_column}
                    WHERE d.{dim_id_column} IS NULL AND f.{fk_column} IS NOT NULL
                """)
                
                rows_affected = cursor.rowcount
                
                if rows_affected > 0:
                    self.logger.info(f"Fixed {rows_affected} invalid {fk_column} references")
            
            # Napraw NULL w wymiarach obowiązkowych
            required_dims = ['date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id']
            
            for dim in required_dims:
                cursor.execute(f"""
                    UPDATE fact_energy_weather
                    SET {dim} = 0
                    WHERE {dim} IS NULL OR {dim} = 0
                """)
                
                rows_affected = cursor.rowcount
                
                if rows_affected > 0:
                    self.logger.info(f"Fixed {rows_affected} NULL {dim} values")
            
            # Napraw ekstremalne wartości temperatury
            cursor.execute("""
                UPDATE fact_energy_weather
                SET temperature_avg = NULL
                WHERE temperature_avg < -80 OR temperature_avg > 80
            """)
            
            rows_affected = cursor.rowcount
            
            if rows_affected > 0:
                self.logger.info(f"Fixed {rows_affected} extreme temperature values")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error fixing data issues: {str(e)}")
            return False
    
    def run_full_etl_process(self) -> bool:
        """
        Wykonanie pełnego procesu ETL
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Starting full ETL process")
        
        # Logowanie rozpoczęcia procesu
        self._log_process('DATA_CLEANING', 'RUNNING')
        
        try:
            # 1. Sprawdzenie dostępnych tabel staging
            staging_tables = self.check_staging_tables()
            self.logger.info(f"Found {len(staging_tables)} staging tables with data")
            
            # 2. Zapewnienie istnienia tabel docelowych
            tables_exist = self.ensure_target_tables_exist()
            if not tables_exist:
                self.logger.error("Failed to ensure target tables exist")
                self._log_process('DATA_CLEANING', 'FAILED', 0, "Failed to ensure target tables exist")
                return False
            
            # 3. Ładowanie wymiarów
            dimensions_loaded = self.load_dimensions_from_staging()
            if not dimensions_loaded:
                self.logger.error("Failed to load dimensions")
                self._log_process('DATA_CLEANING', 'FAILED', 0, "Failed to load dimensions")
                return False
            
            # 4. Ładowanie źródeł
            sources_loaded = self.clean_and_load_source_tables()
            if not sources_loaded:
                self.logger.warning("Issues with loading source tables, but continuing")
            
            # 5. Ładowanie faktów
            facts_loaded = self.load_facts_from_staging()
            if not facts_loaded:
                self.logger.error("Failed to load facts")
                self._log_process('DATA_CLEANING', 'FAILED', 0, "Failed to load facts")
                return False
            
            # 6. Walidacja relacji
            validation_results = self.validate_data_relationships()
            
            # 7. Naprawianie problemów
            if any(count > 0 for count in validation_results.values()):
                self.logger.warning("Found data issues, attempting to fix")
                fixed = self.fix_data_issues()
                if not fixed:
                    self.logger.error("Failed to fix data issues")
                    self._log_process('DATA_CLEANING', 'FAILED', 0, "Failed to fix data issues")
                    return False
                
                # Sprawdź ponownie po naprawie
                validation_results = self.validate_data_relationships()
                if any(count > 0 for count in validation_results.values()):
                    self.logger.warning("Some data issues remain after fixes, but continuing")
            
            # Podsumowanie
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM fact_energy_weather")
            fact_count = cursor.fetchone()[0]
            
            conn.close()
            
            self.logger.info(f"ETL process completed successfully with {fact_count} fact records")
            self._log_process('DATA_CLEANING', 'SUCCESS', fact_count)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during ETL process: {str(e)}")
            self._log_process('DATA_CLEANING', 'FAILED', 0, str(e))
            return False
    
    def _log_process(self, process_name: str, status: str, records: int = 0, error_msg: str = None):
        """Logowanie procesu do bazy danych"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Sprawdź czy procedura istnieje
            try:
                cursor.execute("""
                    SELECT 1 FROM sys.objects WHERE type = 'P' AND name = 'sp_log_etl_process'
                """)
                procedure_exists = cursor.fetchone() is not None
            except:
                procedure_exists = False
            
            if procedure_exists:
                cursor.execute("""
                    EXEC sp_log_etl_process ?, ?, ?, ?
                """, (process_name, status, records, error_msg))
            else:
                # Jeśli procedura nie istnieje, utwórz tabelę logowania i wstaw bezpośrednio
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_process_log')
                    CREATE TABLE etl_process_log (
                        id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        process_name NVARCHAR(100) NOT NULL,
                        status NVARCHAR(20) NOT NULL,
                        records_processed INT NOT NULL DEFAULT 0,
                        error_message NVARCHAR(MAX),
                        start_time DATETIME2 DEFAULT GETDATE(),
                        end_time DATETIME2 DEFAULT GETDATE()
                    )
                """)
                
                cursor.execute("""
                    INSERT INTO etl_process_log (process_name, status, records_processed, error_message)
                    VALUES (?, ?, ?, ?)
                """, (process_name, status, records, error_msg))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging process: {str(e)}")
            # Nie rzucaj wyjątku aby nie przerywać głównego procesu
    def _create_default_fact_table(self, conn):
        """
        Tworzenie domyślnej tabeli faktów, gdy tabela staging nie istnieje
        
        Args:
            conn: Połączenie z bazą danych
        """
        try:
            cursor = conn.cursor()
            
            # Na podstawie zrzutu ekranu widzimy, że kolumna ID to energy_weather_id
            # Standardowa struktura tabeli faktów
            create_sql = """
            CREATE TABLE fact_energy_weather (
                energy_weather_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                date_id INT NOT NULL,
                time_id INT NOT NULL,
                bidding_zone_id INT NOT NULL,
                weather_zone_id INT NOT NULL,
                generation_type_id INT,
                weather_condition_id INT,
                socioeconomic_profile_id INT,
                actual_consumption DECIMAL(18, 2),
                forecasted_consumption DECIMAL(18, 2),
                consumption_deviation DECIMAL(10, 2),
                generation_amount DECIMAL(18, 2),
                capacity_factor DECIMAL(5, 2),
                renewable_percentage DECIMAL(5, 2),
                per_capita_consumption DECIMAL(18, 6),
                temperature_avg DECIMAL(5, 2),
                temperature_min DECIMAL(5, 2),
                temperature_max DECIMAL(5, 2),
                humidity DECIMAL(5, 2),
                precipitation DECIMAL(8, 2),
                wind_speed DECIMAL(5, 2),
                wind_direction INT,
                cloud_cover DECIMAL(5, 2),
                solar_radiation DECIMAL(8, 2),
                air_pressure DECIMAL(8, 2),
                heating_degree_days DECIMAL(5, 2),
                cooling_degree_days DECIMAL(5, 2),
                created_at DATETIME2 DEFAULT GETDATE(),
                CONSTRAINT FK_fact_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
                CONSTRAINT FK_fact_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
                CONSTRAINT FK_fact_bidding_zone FOREIGN KEY (bidding_zone_id) REFERENCES dim_bidding_zone(bidding_zone_id),
                CONSTRAINT FK_fact_weather_zone FOREIGN KEY (weather_zone_id) REFERENCES dim_weather_zone(weather_zone_id),
                CONSTRAINT FK_fact_generation_type FOREIGN KEY (generation_type_id) REFERENCES dim_generation_type(generation_type_id),
                CONSTRAINT FK_fact_weather_condition FOREIGN KEY (weather_condition_id) REFERENCES dim_weather_condition(weather_condition_id),
                CONSTRAINT FK_fact_socioeconomic_profile FOREIGN KEY (socioeconomic_profile_id) REFERENCES dim_socioeconomic_profile(socioeconomic_profile_id)
            )
            """
            
            self.logger.info("Creating default fact_energy_weather table")
            cursor.execute(create_sql)
            conn.commit()
            
            self.logger.info("Default fact table created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating default fact table: {str(e)}")
            conn.rollback()    
    def _get_dimension_id_column(self, conn, dim_table: str) -> str:
        """
        Sprawdza i zwraca nazwę kolumny ID dla tabeli wymiarów
        
        Args:
            conn: Połączenie z bazą danych
            dim_table: Nazwa tabeli wymiarów
            
        Returns:
            Nazwa kolumny ID
        """
        cursor = conn.cursor()
        
        # Na podstawie zrzutów ekranu widzimy, że kolumny id to:
        # dim_date -> date_id
        # dim_time -> time_id
        # dim_bidding_zone -> bidding_zone_id
        # dim_weather_zone -> weather_zone_id
        # dim_generation_type -> generation_type_id
        # dim_weather_condition -> weather_condition_id
        # dim_socioeconomic_profile -> socioeconomic_profile_id
        
        # Mapowanie tabel wymiarowych na kolumny klucza głównego
        dimension_id_map = {
            'dim_date': 'date_id',
            'dim_time': 'time_id',
            'dim_bidding_zone': 'bidding_zone_id',
            'dim_weather_zone': 'weather_zone_id',
            'dim_generation_type': 'generation_type_id',
            'dim_weather_condition': 'weather_condition_id',
            'dim_socioeconomic_profile': 'socioeconomic_profile_id'
        }
        
        # Jeśli mamy znaną tabelę, zwróć jej kolumnę ID
        if dim_table in dimension_id_map:
            id_column = dimension_id_map[dim_table]
            self.logger.info(f"Using mapped ID column for {dim_table}: {id_column}")
            return id_column
        
        # Spróbuj różne możliwe nazwy kolumn
        possible_id_columns = [
            f"{dim_table.replace('dim_', '')}_id",  # date_id
            "id",                                  # id
            f"{dim_table}_id"                      # dim_date_id
        ]
        
        for col_name in possible_id_columns:
            try:
                cursor.execute(f"""
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{dim_table}'
                    AND COLUMN_NAME = '{col_name}'
                """)
                
                if cursor.fetchone():
                    self.logger.info(f"Found ID column for {dim_table}: {col_name}")
                    return col_name
            except:
                continue
        
        # Ostatnia szansa - znajdź pierwszą kolumnę z 'id' w nazwie
        try:
            cursor.execute(f"""
                SELECT TOP 1 COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{dim_table}'
                AND COLUMN_NAME LIKE '%id%'
                ORDER BY ORDINAL_POSITION
            """)
            
            row = cursor.fetchone()
            if row:
                self.logger.info(f"Found ID column for {dim_table} by pattern match: {row.COLUMN_NAME}")
                return row.COLUMN_NAME
        except:
            pass
        
        # Domyślnie
        self.logger.warning(f"Could not determine ID column for {dim_table}, using default: id")
        return "id"

def main():
    """Główna funkcja wywoływana przez SSIS"""
    import sys
    import os
    
    # Konfiguracja logowania
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                       handlers=[
                           logging.FileHandler("data_cleaner.log"),
                           logging.StreamHandler()
                       ])
    logger = logging.getLogger("DataCleanerMain")
    
    logger.info("Starting DataCleaner process")
    
    # Parametry z SSIS lub zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Jeśli nie podano connection_string, użyj domyślnego
    if not connection_string:
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        logger.info(f"Using default connection string: {connection_string}")
    
    try:
        # Inicjalizacja DataCleaner
        cleaner = DataCleaner(connection_string)
        
        # Uruchomienie pełnego procesu ETL
        success = cleaner.run_full_etl_process()
        
        if success:
            logger.info("DataCleaner process completed successfully")
            sys.exit(0)
        else:
            logger.error("DataCleaner process failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()