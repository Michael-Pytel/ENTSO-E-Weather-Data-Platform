"""
RebuildWarehouse.py
Usuwanie i odtwarzanie struktury hurtowni danych
Tworzenie tabel wymiarowych i faktów, ładowanie danych
"""

import pyodbc
import logging
import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd

class WarehouseBuilder:
    """Klasa do przebudowy hurtowni danych"""
    
    def __init__(self, connection_string: str):
        """
        Inicjalizacja
        
        Args:
            connection_string: String połączenia z bazą danych
        """
        self.connection_string = connection_string
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Lista tabel do usunięcia
        self.tables_to_drop = [
            # Tabele faktów
            'fact_energy_weather',
            # Tabele wymiarów
            'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
            'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile',
            # Tabele źródłowe
            'src_entso_actual_load', 'src_entso_generation', 'src_entso_forecast',
            'src_weather_data', 'src_climate_data', 'src_eurostat_integrated'
        ]
        
    def drop_tables(self) -> bool:
        """
        Usuwanie istniejących tabel
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Dropping existing tables")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Najpierw usuń ograniczenia kluczy obcych
            self.logger.info("Dropping foreign key constraints")
            
            cursor.execute("""
                SELECT 
                    fk.name AS FK_NAME,
                    OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME
                FROM 
                    sys.foreign_keys AS fk
                WHERE 
                    OBJECT_NAME(fk.referenced_object_id) IN (
                        'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
                        'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile'
                    )
            """)
            
            foreign_keys = cursor.fetchall()
            
            for fk in foreign_keys:
                fk_name = fk.FK_NAME
                table_name = fk.TABLE_NAME
                
                self.logger.info(f"Dropping foreign key {fk_name} from {table_name}")
                cursor.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {fk_name}")
                conn.commit()
            
            # Teraz usuń tabele
            for table in self.tables_to_drop:
                try:
                    cursor.execute(f"""
                        IF OBJECT_ID('{table}', 'U') IS NOT NULL
                            DROP TABLE {table}
                    """)
                    conn.commit()
                    self.logger.info(f"Dropped table {table}")
                except Exception as e:
                    self.logger.error(f"Error dropping table {table}: {str(e)}")
                    conn.rollback()
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error dropping tables: {str(e)}")
            return False
    
    def create_dimension_tables(self) -> bool:
        """
        Tworzenie tabel wymiarowych
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating dimension tables")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Tabela wymiaru Date
            self.logger.info("Creating dim_date table")
            cursor.execute("""
                CREATE TABLE dim_date (
                    date_id INT IDENTITY(0,1) PRIMARY KEY,
                    full_date DATE NOT NULL,
                    day_of_week VARCHAR(10),
                    day_of_month INT,
                    month INT,
                    month_name VARCHAR(20),
                    quarter INT,
                    year INT,
                    season VARCHAR(20),
                    is_holiday VARCHAR(3),
                    holiday_name VARCHAR(100),
                    holiday_type VARCHAR(50),
                    is_school_day VARCHAR(3),
                    is_weekend VARCHAR(3),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela wymiaru Time
            self.logger.info("Creating dim_time table")
            cursor.execute("""
                CREATE TABLE dim_time (
                    time_id INT IDENTITY(0,1) PRIMARY KEY,
                    hour INT,
                    minute INT,
                    day_period VARCHAR(10),
                    is_peak_hour VARCHAR(3),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela wymiaru Bidding Zone
            self.logger.info("Creating dim_bidding_zone table")
            cursor.execute("""
                CREATE TABLE dim_bidding_zone (
                    bidding_zone_id INT IDENTITY(0,1) PRIMARY KEY,
                    bidding_zone_code VARCHAR(50),
                    bidding_zone_name VARCHAR(100),
                    primary_country VARCHAR(50),
                    secondary_countries VARCHAR(100),
                    control_area VARCHAR(50),
                    timezone VARCHAR(50),
                    population BIGINT,
                    gdp_per_capita DECIMAL(15, 2),
                    energy_intensity DECIMAL(15, 2),
                    electricity_price_avg DECIMAL(10, 2),
                    year INT,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela wymiaru Weather Zone
            self.logger.info("Creating dim_weather_zone table")
            cursor.execute("""
                CREATE TABLE dim_weather_zone (
                    weather_zone_id INT IDENTITY(0,1) PRIMARY KEY,
                    weather_zone_name VARCHAR(100),
                    bidding_zone_id INT,
                    climate_zone VARCHAR(50),
                    elevation_avg DECIMAL(8, 2),
                    coastal_proximity VARCHAR(20),
                    urbanization_level VARCHAR(20),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela wymiaru Generation Type
            self.logger.info("Creating dim_generation_type table")
            cursor.execute("""
                CREATE TABLE dim_generation_type (
                    generation_type_id INT IDENTITY(0,1) PRIMARY KEY,
                    entso_code VARCHAR(5),
                    generation_category VARCHAR(50),
                    generation_type VARCHAR(50),
                    is_intermittent VARCHAR(3),
                    fuel_source VARCHAR(50),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela wymiaru Weather Condition
            self.logger.info("Creating dim_weather_condition table")
            cursor.execute("""
                CREATE TABLE dim_weather_condition (
                    weather_condition_id INT IDENTITY(0,1) PRIMARY KEY,
                    condition_type VARCHAR(30),
                    condition_severity VARCHAR(20),
                    is_extreme_weather VARCHAR(3),
                    extreme_weather_type VARCHAR(30),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela wymiaru Socioeconomic Profile
            self.logger.info("Creating dim_socioeconomic_profile table")
            cursor.execute("""
                CREATE TABLE dim_socioeconomic_profile (
                    socioeconomic_profile_id INT IDENTITY(0,1) PRIMARY KEY,
                    bidding_zone_code VARCHAR(50),
                    country_code VARCHAR(5),
                    country_name VARCHAR(100),
                    year INT,
                    avg_income_level DECIMAL(12, 2),
                    unemployment_rate DECIMAL(5, 2),
                    urbanization_rate DECIMAL(5, 2),
                    service_sector_percentage DECIMAL(5, 2),
                    industry_sector_percentage DECIMAL(5, 2),
                    energy_poverty_rate DECIMAL(5, 2),
                    residential_percentage DECIMAL(5, 2),
                    commercial_percentage DECIMAL(5, 2),
                    industrial_percentage DECIMAL(5, 2),
                    avg_household_size DECIMAL(5, 2),
                    primary_heating_type VARCHAR(50),
                    population BIGINT,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating dimension tables: {str(e)}")
            return False
    
    def create_fact_table(self) -> bool:
        """
        Tworzenie tabeli faktów
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating fact table")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Tabela faktów Energy Weather
            cursor.execute("""
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
            """)
            conn.commit()
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating fact table: {str(e)}")
            return False
    
    def create_source_tables(self) -> bool:
        """
        Tworzenie tabel źródłowych
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating source tables")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Tabela ENTSO-E Load
            cursor.execute("""
                CREATE TABLE src_entso_actual_load (
                    entso_actual_load_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    timestamp DATETIME2,
                    quantity DECIMAL(15, 2),
                    bidding_zone VARCHAR(50),
                    zone_code VARCHAR(50),
                    country VARCHAR(5),
                    data_type VARCHAR(20),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela ENTSO-E Generation
            cursor.execute("""
                CREATE TABLE src_entso_generation (
                    entso_generation_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    timestamp DATETIME2,
                    quantity DECIMAL(15, 2),
                    bidding_zone VARCHAR(50),
                    zone_code VARCHAR(50),
                    country VARCHAR(5),
                    generation_type VARCHAR(5),
                    data_type VARCHAR(20),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela ENTSO-E Forecast
            cursor.execute("""
                CREATE TABLE src_entso_forecast (
                    entso_forecast_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    timestamp DATETIME2,
                    quantity DECIMAL(15, 2),
                    bidding_zone VARCHAR(50),
                    zone_code VARCHAR(50),
                    country VARCHAR(5),
                    data_type VARCHAR(20),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela Weather Data
            cursor.execute("""
                CREATE TABLE src_weather_data (
                    weather_data_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    timestamp DATETIME2,
                    subzone_code VARCHAR(10),
                    subzone_name VARCHAR(100),
                    country_code VARCHAR(5),
                    zone_name VARCHAR(100),
                    temperature_avg DECIMAL(5, 2),
                    temperature_min DECIMAL(5, 2),
                    temperature_max DECIMAL(5, 2),
                    humidity DECIMAL(5, 2),
                    precipitation DECIMAL(5, 2),
                    wind_speed DECIMAL(5, 2),
                    wind_direction INT,
                    air_pressure DECIMAL(8, 2),
                    cloud_cover DECIMAL(5, 2),
                    solar_radiation DECIMAL(8, 2),
                    weather_condition VARCHAR(30),
                    latitude DECIMAL(10, 6),
                    longitude DECIMAL(10, 6),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela Climate Data
            cursor.execute("""
                CREATE TABLE src_climate_data (
                    climate_data_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    date DATE,
                    subzone_code VARCHAR(10),
                    subzone_name VARCHAR(100),
                    country_code VARCHAR(5),
                    zone_name VARCHAR(100),
                    heating_degree_days DECIMAL(5, 2),
                    cooling_degree_days DECIMAL(5, 2),
                    temperature_mean DECIMAL(5, 2),
                    temperature_min DECIMAL(5, 2),
                    temperature_max DECIMAL(5, 2),
                    latitude DECIMAL(10, 6),
                    longitude DECIMAL(10, 6),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            # Tabela Eurostat Integrated
            cursor.execute("""
                CREATE TABLE src_eurostat_integrated (
                    eurostat_integrated_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    country_code VARCHAR(5),
                    country_name VARCHAR(100),
                    year INT,
                    population BIGINT,
                    gdp_per_capita DECIMAL(15, 2),
                    electricity_price_avg DECIMAL(10, 2),
                    energy_intensity DECIMAL(15, 2),
                    unemployment_rate DECIMAL(5, 2),
                    poverty_by_degree_of_urbanization DECIMAL(5, 2),
                    service_sector_percentage DECIMAL(5, 2),
                    industry_sector_percentage DECIMAL(5, 2),
                    avg_household_size DECIMAL(5, 2),
                    energy_poverty_rate DECIMAL(5, 2),
                    primary_heating_type VARCHAR(50),
                    data_quality_score INT,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            conn.commit()
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating source tables: {str(e)}")
            return False
    
    def insert_default_dimension_records(self) -> bool:
        """
        Wstawianie domyślnych rekordów do wymiarów (ID=0)
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Inserting default dimension records")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Domyślny rekord dla dim_date
            self.logger.info("Inserting default record for dim_date")
            cursor.execute("""
                SET IDENTITY_INSERT dim_date ON;
                INSERT INTO dim_date (date_id, full_date, day_of_week, day_of_month, month, month_name, quarter, year, season, is_holiday, holiday_name, holiday_type, is_school_day, is_weekend)
                VALUES (0, '1900-01-01', 'Unknown', 0, 0, 'Unknown', 0, 0, 'Unknown', 'No', 'None', 'None', 'No', 'No');
                SET IDENTITY_INSERT dim_date OFF;
            """)
            conn.commit()
            
            # Domyślny rekord dla dim_time
            self.logger.info("Inserting default record for dim_time")
            cursor.execute("""
                SET IDENTITY_INSERT dim_time ON;
                INSERT INTO dim_time (time_id, hour, minute, day_period, is_peak_hour)
                VALUES (0, 0, 0, 'Unknown', 'No');
                SET IDENTITY_INSERT dim_time OFF;
            """)
            conn.commit()
            
            # Domyślny rekord dla dim_bidding_zone
            self.logger.info("Inserting default record for dim_bidding_zone")
            cursor.execute("""
                SET IDENTITY_INSERT dim_bidding_zone ON;
                INSERT INTO dim_bidding_zone (bidding_zone_id, bidding_zone_code, bidding_zone_name, primary_country, secondary_countries, control_area, timezone, population, gdp_per_capita, energy_intensity, electricity_price_avg, year)
                VALUES (0, 'UNKNOWN', 'Unknown', 'UNK', 'None', 'Unknown', 'UTC', 0, 0.0, 0.0, 0.0, 0);
                SET IDENTITY_INSERT dim_bidding_zone OFF;
            """)
            conn.commit()
            
            # Domyślny rekord dla dim_weather_zone
            self.logger.info("Inserting default record for dim_weather_zone")
            cursor.execute("""
                SET IDENTITY_INSERT dim_weather_zone ON;
                INSERT INTO dim_weather_zone (weather_zone_id, weather_zone_name, bidding_zone_id, climate_zone, elevation_avg, coastal_proximity, urbanization_level)
                VALUES (0, 'Unknown', 0, 'Unknown', 0.0, 'Unknown', 'Unknown');
                SET IDENTITY_INSERT dim_weather_zone OFF;
            """)
            conn.commit()
            
            # Domyślny rekord dla dim_generation_type
            self.logger.info("Inserting default record for dim_generation_type")
            cursor.execute("""
                SET IDENTITY_INSERT dim_generation_type ON;
                INSERT INTO dim_generation_type (generation_type_id, entso_code, generation_category, generation_type, is_intermittent, fuel_source)
                VALUES (0, 'B20', 'Unknown', 'Unknown', 'No', 'Unknown');
                SET IDENTITY_INSERT dim_generation_type OFF;
            """)
            conn.commit()
            
            # Domyślny rekord dla dim_weather_condition
            self.logger.info("Inserting default record for dim_weather_condition")
            cursor.execute("""
                SET IDENTITY_INSERT dim_weather_condition ON;
                INSERT INTO dim_weather_condition (weather_condition_id, condition_type, condition_severity, is_extreme_weather, extreme_weather_type)
                VALUES (0, 'Unknown', 'None', 'No', 'None');
                SET IDENTITY_INSERT dim_weather_condition OFF;
            """)
            conn.commit()
            
            # Domyślny rekord dla dim_socioeconomic_profile
            self.logger.info("Inserting default record for dim_socioeconomic_profile")
            cursor.execute("""
                SET IDENTITY_INSERT dim_socioeconomic_profile ON;
                INSERT INTO dim_socioeconomic_profile (socioeconomic_profile_id, bidding_zone_code, country_code, country_name, year, avg_income_level, unemployment_rate, urbanization_rate, service_sector_percentage, industry_sector_percentage, energy_poverty_rate, residential_percentage, commercial_percentage, industrial_percentage, avg_household_size, primary_heating_type, population)
                VALUES (0, 'UNKNOWN', 'UNK', 'Unknown', 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 'Unknown', 0);
                SET IDENTITY_INSERT dim_socioeconomic_profile OFF;
            """)
            conn.commit()
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting default dimension records: {str(e)}")
            return False
    
    def load_data_from_staging(self) -> bool:
        """
        Ładowanie danych z tabel staging do docelowych - zoptymalizowana wersja
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Loading data from staging")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Najpierw załaduj fakty, żeby nie czekać na nie na końcu
            self.logger.info("Loading fact data first (optimized)")
            self._load_fact_data(conn)
            
            # Ładowanie danych wymiarów - równolegle
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
                    
                    # Zoptymalizowane wstawianie - określ bezpośrednio potrzebne kolumny
                    # zamiast znajdowania ich dynamicznie
                    
                    # Typowe kolumny dla każdego wymiaru
                    dimension_columns = {
                        'dim_date': ['full_date', 'day_of_week', 'day_of_month', 'month', 'month_name', 
                                    'quarter', 'year', 'season', 'is_holiday', 'holiday_name', 
                                    'holiday_type', 'is_school_day', 'is_weekend'],
                        'dim_time': ['hour', 'minute', 'day_period', 'is_peak_hour'],
                        'dim_bidding_zone': ['bidding_zone_code', 'bidding_zone_name', 'primary_country', 
                                           'secondary_countries', 'control_area', 'timezone', 'population', 
                                           'gdp_per_capita', 'energy_intensity', 'electricity_price_avg', 'year'],
                        'dim_weather_zone': ['weather_zone_name', 'bidding_zone_id', 'climate_zone', 
                                           'elevation_avg', 'coastal_proximity', 'urbanization_level'],
                        'dim_generation_type': ['entso_code', 'generation_category', 'generation_type', 
                                              'is_intermittent', 'fuel_source'],
                        'dim_weather_condition': ['condition_type', 'condition_severity', 
                                                'is_extreme_weather', 'extreme_weather_type'],
                        'dim_socioeconomic_profile': ['bidding_zone_code', 'country_code', 'country_name', 
                                                    'year', 'avg_income_level', 'unemployment_rate', 
                                                    'urbanization_rate', 'service_sector_percentage', 
                                                    'industry_sector_percentage', 'energy_poverty_rate', 
                                                    'residential_percentage', 'commercial_percentage', 
                                                    'industrial_percentage', 'avg_household_size', 
                                                    'primary_heating_type', 'population']
                    }
                    
                    # Pobierz kolumny z tabeli staging
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{staging_table}'
                        AND COLUMN_NAME <> 'id'
                        AND COLUMN_NAME <> 'created_at'
                    """)
                    
                    staging_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Znajdź wspólne kolumny
                    expected_columns = dimension_columns.get(dim, [])
                    common_columns = [col for col in expected_columns if col in staging_columns]
                    
                    if common_columns:
                        # Wstaw dane w jednej transakcji
                        insert_sql = f"""
                        INSERT INTO {dim} ({', '.join(common_columns)})
                        SELECT {', '.join(common_columns)}
                        FROM {staging_table}
                        """
                        
                        cursor.execute(insert_sql)
                        conn.commit()
                        
                        # Sprawdź liczbę wstawionych rekordów
                        cursor.execute(f"SELECT COUNT(*) FROM {dim}")
                        record_count = cursor.fetchone()[0]
                        
                        self.logger.info(f"Successfully loaded {record_count - 1} records to {dim}")
                    else:
                        self.logger.warning(f"No common columns found between {staging_table} and {dim}")
                else:
                    self.logger.warning(f"No data in {staging_table}, skipping {dim}")
            
            # Ładowanie danych źródłowych - zoptymalizowane
            source_tables = [
                'src_entso_actual_load', 'src_entso_generation', 'src_entso_forecast',
                'src_weather_data', 'src_climate_data', 'src_eurostat_integrated'
            ]
            
            for src_table in source_tables:
                staging_table = f"staging_{src_table.replace('src_', '')}"
                
                # Sprawdź czy tabela staging istnieje i ma dane
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                        SELECT COUNT(*) FROM {staging_table}
                    ELSE
                        SELECT 0
                """)
                
                staging_count = cursor.fetchone()[0]
                
                if staging_count > 0:
                    self.logger.info(f"Loading {staging_count} records from {staging_table} to {src_table}")
                    
                    # Pobierz kolumny z tabeli docelowej (bez _id i created_at)
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{src_table}'
                        AND COLUMN_NAME NOT LIKE '%_id'
                        AND COLUMN_NAME <> 'created_at'
                    """)
                    
                    target_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Pobierz kolumny z tabeli staging
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{staging_table}'
                        AND COLUMN_NAME <> 'id'
                        AND COLUMN_NAME <> 'created_at'
                    """)
                    
                    staging_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                    
                    # Znajdź wspólne kolumny
                    common_columns = [col for col in staging_columns if col in target_columns]
                    
                    if common_columns:
                        # Zastosuj zoptymalizowane wstawianie partiami dla dużych tabel
                        if staging_count > 10000:
                            self.logger.info(f"Using batch insert for large table {src_table}")
                            
                            batch_size = 10000
                            batch_count = (staging_count + batch_size - 1) // batch_size
                            
                            for i in range(batch_count):
                                start_offset = i * batch_size
                                
                                # Wstaw partię danych
                                batch_insert_sql = f"""
                                INSERT INTO {src_table} ({', '.join(common_columns)})
                                SELECT TOP {batch_size} {', '.join(common_columns)}
                                FROM {staging_table}
                                ORDER BY (SELECT NULL)
                                OFFSET {start_offset} ROWS
                                """
                                
                                cursor.execute(batch_insert_sql)
                                conn.commit()
                                
                                self.logger.info(f"Inserted batch {i+1}/{batch_count} for {src_table}")
                        else:
                            # Dla mniejszych tabel wstaw wszystko na raz
                            insert_sql = f"""
                            INSERT INTO {src_table} ({', '.join(common_columns)})
                            SELECT {', '.join(common_columns)}
                            FROM {staging_table}
                            """
                            
                            cursor.execute(insert_sql)
                            conn.commit()
                        
                        # Sprawdź liczbę wstawionych rekordów
                        cursor.execute(f"SELECT COUNT(*) FROM {src_table}")
                        record_count = cursor.fetchone()[0]
                        
                        self.logger.info(f"Successfully loaded {record_count} records to {src_table}")
                    else:
                        self.logger.warning(f"No common columns found between {staging_table} and {src_table}")
                else:
                    self.logger.warning(f"No data in {staging_table}, skipping {src_table}")
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data from staging: {str(e)}")
            return False
    
    def _load_fact_data(self, conn):
        """
        Ładowanie danych faktów - zoptymalizowana wersja
        
        Args:
            conn: Połączenie z bazą danych
        """
        cursor = conn.cursor()
        
        # Sprawdź czy tabela staging faktów istnieje i ma dane
        cursor.execute("""
            IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL
                SELECT COUNT(*) FROM staging_fact_energy_weather
            ELSE
                SELECT 0
        """)
        
        staging_count = cursor.fetchone()[0]
        
        if staging_count > 0:
            self.logger.info(f"Loading {staging_count} records from staging_fact_energy_weather to fact_energy_weather")
            
            # Zamiast znajdować wspólne kolumny, użyjmy bezpośrednio kolumn, które wiemy, że są potrzebne
            # To przyspieszy proces
            fact_columns = [
                'date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id', 'generation_type_id', 
                'weather_condition_id', 'socioeconomic_profile_id', 'actual_consumption', 
                'forecasted_consumption', 'consumption_deviation', 'generation_amount', 
                'capacity_factor', 'renewable_percentage', 'per_capita_consumption', 
                'temperature_avg', 'temperature_min', 'temperature_max', 'humidity', 
                'precipitation', 'wind_speed', 'wind_direction', 'cloud_cover', 
                'solar_radiation', 'air_pressure', 'heating_degree_days', 'cooling_degree_days'
            ]
            
            # Wybierz tylko kolumny, które istnieją w tabeli staging
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'staging_fact_energy_weather'
            """)
            
            staging_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
            existing_columns = [col for col in fact_columns if col in staging_columns]
            
            # Dla kluczy obcych, zastąp NULL-e zerami (referencja do domyślnych rekordów)
            fk_columns = ['date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id', 
                         'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id']
            
            select_columns = []
            for col in existing_columns:
                if col in fk_columns:
                    select_columns.append(f"ISNULL({col}, 0) AS {col}")
                else:
                    select_columns.append(col)
            
            # Zoptymalizowane wstawianie - użyj bulk insert z ograniczoną transakcją
            # i zwiększoną wydajnością
            
            # Tymczasowo wyłącz ograniczenia kluczy obcych
            self.logger.info("Temporarily disabling foreign key constraints")
            cursor.execute("""
                ALTER TABLE fact_energy_weather NOCHECK CONSTRAINT ALL
            """)
            conn.commit()
            
            # Użyj tabelarycznego podejścia do wstawiania dla lepszej wydajności
            batch_size = 50000  # Zwiększony rozmiar wsadu
            
            self.logger.info(f"Starting optimized batch insert with batch size {batch_size}")
            
            # Pobierz całkowitą liczbę rekordów
            cursor.execute("SELECT COUNT(*) FROM staging_fact_energy_weather")
            total_records = cursor.fetchone()[0]
            batch_count = (total_records + batch_size - 1) // batch_size
            
            for i in range(batch_count):
                start_offset = i * batch_size
                
                self.logger.info(f"Processing batch {i+1}/{batch_count} (offset {start_offset})")
                
                # Wstaw partię danych
                batch_insert_sql = f"""
                INSERT INTO fact_energy_weather ({', '.join(existing_columns)})
                SELECT TOP {batch_size} {', '.join(select_columns)}
                FROM staging_fact_energy_weather
                ORDER BY (SELECT NULL)
                OFFSET {start_offset} ROWS
                """
                
                cursor.execute(batch_insert_sql)
                conn.commit()
                
                self.logger.info(f"Inserted batch {i+1}/{batch_count}")
            
            # Włącz ponownie ograniczenia kluczy obcych
            self.logger.info("Re-enabling foreign key constraints")
            cursor.execute("""
                ALTER TABLE fact_energy_weather WITH CHECK CHECK CONSTRAINT ALL
            """)
            conn.commit()
            
            # Sprawdź liczbę wstawionych rekordów
            cursor.execute("SELECT COUNT(*) FROM fact_energy_weather")
            record_count = cursor.fetchone()[0]
            
            self.logger.info(f"Successfully loaded {record_count} records to fact_energy_weather")
        else:
            self.logger.warning("No data in staging_fact_energy_weather, skipping fact table")
    
    def run_full_rebuild(self) -> bool:
        """
        Przeprowadzenie pełnej przebudowy hurtowni
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Starting full warehouse rebuild")
        
        # 1. Usunięcie istniejących tabel
        if not self.drop_tables():
            self.logger.error("Failed to drop existing tables")
            return False
        
        # 2. Utworzenie tabel wymiarowych
        if not self.create_dimension_tables():
            self.logger.error("Failed to create dimension tables")
            return False
        
        # 3. Wstawienie domyślnych rekordów wymiarowych
        if not self.insert_default_dimension_records():
            self.logger.error("Failed to insert default dimension records")
            return False
        
        # 4. Utworzenie tabel źródłowych
        if not self.create_source_tables():
            self.logger.error("Failed to create source tables")
            return False
        
        # 5. Utworzenie tabeli faktów
        if not self.create_fact_table():
            self.logger.error("Failed to create fact table")
            return False
        
        # 6. Załadowanie danych z tabel staging
        if not self.load_data_from_staging():
            self.logger.error("Failed to load data from staging")
            return False
        
        self.logger.info("Warehouse rebuild completed successfully")
        return True

def main():
    """Główna funkcja wywoływana z konsoli"""
    import sys
    import os
    
    # Konfiguracja logowania
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                       handlers=[
                           logging.FileHandler("rebuild_warehouse.log"),
                           logging.StreamHandler()
                       ])
    logger = logging.getLogger("WarehouseBuilderMain")
    
    logger.info("Starting Warehouse Rebuild")
    
    # Parametry z zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Jeśli nie podano connection_string, użyj domyślnego
    if not connection_string:
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        logger.info(f"Using default connection string: {connection_string}")
    
    try:
        # Inicjalizacja WarehouseBuilder
        builder = WarehouseBuilder(connection_string)
        
        # Uruchomienie pełnej przebudowy
        success = builder.run_full_rebuild()
        
        if success:
            logger.info("Warehouse rebuild completed successfully")
            sys.exit(0)
        else:
            logger.error("Warehouse rebuild failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()