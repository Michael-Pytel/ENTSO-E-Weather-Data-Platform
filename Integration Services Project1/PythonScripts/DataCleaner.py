"""
OptimizedWarehouseBuilder.py
Optimized version for memory efficiency and runtime performance
Usuwanie i odtwarzanie struktury hurtowni danych z optymalizacjami
"""

import pyodbc
import logging
import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import List, Tuple, Optional
import time

class OptimizedWarehouseBuilder:
    """Zoptymalizowana klasa do przebudowy hurtowni danych"""
    
    def __init__(self, connection_string: str, max_connections: int = 5, batch_size: int = 10000):
        """
        Inicjalizacja z pool połączeń - bez timeout dla długich operacji
        
        Args:
            connection_string: String połączenia z bazą danych
            max_connections: Maksymalna liczba połączeń w pool
            batch_size: Rozmiar batch dla operacji bulk
        """
        self.connection_string = connection_string
        self.max_connections = max_connections
        self.batch_size = batch_size
        self._connection_pool = []
        self._pool_lock = threading.Lock()
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Initialized WarehouseBuilder - No timeouts (unlimited execution time)")
        
        # Optymalizowane listy tabel
        self.dimension_tables = [
            'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
            'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile'
        ]
        
        self.source_tables = [
            'src_entso_actual_load', 'src_entso_generation', 'src_entso_forecast',
            'src_weather_data', 'src_climate_data', 'src_eurostat_integrated'
        ]
        
        self.fact_tables = ['fact_energy_weather']
        
        self.all_tables = self.fact_tables + self.dimension_tables + self.source_tables
        
        # Pre-compiled SQL statements for better performance
        self._prepare_sql_statements()
    
    def _prepare_sql_statements(self):
        """Przygotowanie często używanych zapytań SQL"""
        self.sql_statements = {
            'check_table_exists': "SELECT 1 FROM sys.tables WHERE name = ?",
            'get_table_count': "SELECT COUNT(*) FROM {}",
            'drop_table': "IF OBJECT_ID('{}', 'U') IS NOT NULL DROP TABLE {}",
            'drop_foreign_key': "ALTER TABLE {} DROP CONSTRAINT {}",
            'get_common_columns': """
                SELECT c1.COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS c1
                INNER JOIN INFORMATION_SCHEMA.COLUMNS c2 
                    ON c1.COLUMN_NAME = c2.COLUMN_NAME
                WHERE c1.TABLE_NAME = ? AND c2.TABLE_NAME = ?
                AND c1.COLUMN_NAME NOT IN ('id', 'created_at')
                AND c2.COLUMN_NAME != 'created_at'
                AND c2.COLUMN_NAME NOT IN (
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = ? 
                    AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
                )
            """,
            'get_common_columns_fact': """
                SELECT c1.COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS c1
                INNER JOIN INFORMATION_SCHEMA.COLUMNS c2 
                    ON c1.COLUMN_NAME = c2.COLUMN_NAME
                WHERE c1.TABLE_NAME = ? AND c2.TABLE_NAME = ?
                AND c1.COLUMN_NAME NOT IN ('id', 'created_at')
                AND c2.COLUMN_NAME != 'created_at'
                AND c2.COLUMN_NAME != 'energy_weather_id'
            """
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager dla pool połączeń - bez timeout"""
        conn = None
        try:
            with self._pool_lock:
                if self._connection_pool:
                    conn = self._connection_pool.pop()
                    self.logger.debug(f"Reused connection from pool. Pool size: {len(self._connection_pool)}")
                else:
                    conn = pyodbc.connect(self.connection_string)
                    self.logger.debug("Created new connection")
            
            # Konfiguracja połączenia - BEZ timeout (unlimited)
            conn.autocommit = False
            conn.timeout = 0  # 0 = unlimited timeout
            
            yield conn
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise e
        finally:
            if conn:
                try:
                    conn.commit()
                    with self._pool_lock:
                        if len(self._connection_pool) < self.max_connections:
                            self._connection_pool.append(conn)
                        else:
                            conn.close()
                except:
                    conn.close()
    
    def close_connection_pool(self):
        """Zamknięcie wszystkich połączeń w pool"""
        with self._pool_lock:
            for conn in self._connection_pool:
                try:
                    conn.close()
                except:
                    pass
            self._connection_pool.clear()
    
    def drop_tables(self) -> bool:
        """
        Optymalizowane usuwanie tabel z lepszym error handling
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Dropping existing tables (optimized)")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Najpierw spróbuj usunąć ograniczenia FK
                self.logger.info("Attempting to drop foreign key constraints")
                
                try:
                    # Uproszczone zapytanie FK z timeout handling
                    cursor.execute("""
                        SELECT 
                            CONSTRAINT_NAME, 
                            TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                        WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'
                        AND TABLE_NAME IN ('fact_energy_weather')
                    """)
                    
                    foreign_keys = cursor.fetchall()
                    
                    # Usuń FK constraints
                    for fk in foreign_keys:
                        try:
                            drop_fk_sql = f"ALTER TABLE {fk.TABLE_NAME} DROP CONSTRAINT {fk.CONSTRAINT_NAME}"
                            cursor.execute(drop_fk_sql)
                            self.logger.debug(f"Dropped FK constraint {fk.CONSTRAINT_NAME}")
                        except Exception as fk_error:
                            self.logger.warning(f"Could not drop FK {fk.CONSTRAINT_NAME}: {fk_error}")
                    
                    if foreign_keys:
                        self.logger.info(f"Processed {len(foreign_keys)} foreign key constraints")
                    else:
                        self.logger.info("No foreign key constraints found")
                        
                except Exception as fk_error:
                    self.logger.warning(f"Could not query/drop FK constraints: {fk_error}")
                    self.logger.info("Proceeding with table drops anyway...")
                
                # Usuń tabele w odpowiedniej kolejności z lepszym error handling
                drop_order = self.fact_tables + self.dimension_tables + self.source_tables
                dropped_count = 0
                
                for table in drop_order:
                    try:
                        # Sprawdź czy tabela istnieje przed usunięciem
                        cursor.execute(f"SELECT 1 FROM sys.tables WHERE name = '{table}'")
                        if cursor.fetchone():
                            cursor.execute(f"DROP TABLE {table}")
                            dropped_count += 1
                            self.logger.debug(f"Dropped table {table}")
                        else:
                            self.logger.debug(f"Table {table} does not exist, skipping")
                    except Exception as table_error:
                        self.logger.warning(f"Could not drop table {table}: {table_error}")
                
                conn.commit()
                self.logger.info(f"Successfully processed {dropped_count} tables")
                return True
                
        except Exception as e:
            self.logger.error(f"Error dropping tables: {str(e)}")
            # Try alternative approach
            return self._alternative_drop_tables()
    
    def _alternative_drop_tables(self) -> bool:
        """
        Alternatywna metoda usuwania tabel gdy główna metoda zawiedzie
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Trying alternative table drop approach")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Najpierw spróbuj usunąć wszystkie FK constraints bez sprawdzania
                fact_table = 'fact_energy_weather'
                potential_constraints = [
                    'FK_fact_date', 'FK_fact_time', 'FK_fact_bidding_zone', 
                    'FK_fact_weather_zone', 'FK_fact_generation_type', 
                    'FK_fact_weather_condition', 'FK_fact_socioeconomic_profile'
                ]
                
                for constraint_name in potential_constraints:
                    try:
                        cursor.execute(f"ALTER TABLE {fact_table} DROP CONSTRAINT {constraint_name}")
                        self.logger.debug(f"Dropped constraint {constraint_name}")
                    except:
                        pass  # Ignore if constraint doesn't exist
                
                # Usuń tabele po jednej z IGNORE błędów
                all_potential_tables = [
                    'fact_energy_weather',
                    'dim_date', 'dim_time', 'dim_bidding_zone', 'dim_weather_zone',
                    'dim_generation_type', 'dim_weather_condition', 'dim_socioeconomic_profile',
                    'src_entso_actual_load', 'src_entso_generation', 'src_entso_forecast',
                    'src_weather_data', 'src_climate_data', 'src_eurostat_integrated'
                ]
                
                dropped_count = 0
                for table in all_potential_tables:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {table}")
                        dropped_count += 1
                        self.logger.debug(f"Dropped table {table}")
                    except Exception as e:
                        # SQL Server może nie obsługiwać IF EXISTS, spróbuj bez tego
                        try:
                            cursor.execute(f"""
                                IF OBJECT_ID('{table}', 'U') IS NOT NULL
                                    DROP TABLE {table}
                            """)
                            dropped_count += 1
                            self.logger.debug(f"Dropped table {table} (fallback method)")
                        except:
                            self.logger.debug(f"Table {table} could not be dropped or doesn't exist")
                
                conn.commit()
                self.logger.info(f"Alternative drop completed, processed {dropped_count} tables")
                return True
                
        except Exception as e:
            self.logger.error(f"Alternative drop method also failed: {str(e)}")
            self.logger.warning("Proceeding with rebuild anyway - tables may not have existed")
            return True  # Return True to continue with rebuild
    
    def create_dimension_tables(self) -> bool:
        """
        Optymalizowane tworzenie tabel wymiarowych z DDL batch
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating dimension tables (optimized)")
        
        # DDL statements organized for batch execution
        ddl_statements = {
            'dim_date': """
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
            """,
            'dim_time': """
                CREATE TABLE dim_time (
                    time_id INT IDENTITY(0,1) PRIMARY KEY,
                    hour INT,
                    minute INT,
                    day_period VARCHAR(10),
                    is_peak_hour VARCHAR(3),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'dim_bidding_zone': """
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
            """,
            'dim_weather_zone': """
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
            """,
            'dim_generation_type': """
                CREATE TABLE dim_generation_type (
                    generation_type_id INT IDENTITY(0,1) PRIMARY KEY,
                    entso_code VARCHAR(5),
                    generation_category VARCHAR(50),
                    generation_type VARCHAR(50),
                    is_intermittent VARCHAR(3),
                    fuel_source VARCHAR(50),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'dim_weather_condition': """
                CREATE TABLE dim_weather_condition (
                    weather_condition_id INT IDENTITY(0,1) PRIMARY KEY,
                    condition_type VARCHAR(30),
                    condition_severity VARCHAR(20),
                    is_extreme_weather VARCHAR(3),
                    extreme_weather_type VARCHAR(30),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'dim_socioeconomic_profile': """
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
            """
        }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Wykonaj wszystkie DDL w jednej transakcji
                for table_name, ddl in ddl_statements.items():
                    self.logger.debug(f"Creating {table_name}")
                    cursor.execute(ddl)
                
                conn.commit()
                self.logger.info(f"Successfully created {len(ddl_statements)} dimension tables")
                return True
                
        except Exception as e:
            self.logger.error(f"Error creating dimension tables: {str(e)}")
            return False
    
    def create_fact_table(self) -> bool:
        """
        Tworzenie tabeli faktów z optymalizacjami
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating fact table (optimized)")
        
        fact_ddl = """
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
                created_at DATETIME2 DEFAULT GETDATE()
            )
        """
        
        # Foreign key constraints będą dodane osobno dla lepszej wydajności
        fk_constraints = [
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id)",
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)",
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_bidding_zone FOREIGN KEY (bidding_zone_id) REFERENCES dim_bidding_zone(bidding_zone_id)",
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_weather_zone FOREIGN KEY (weather_zone_id) REFERENCES dim_weather_zone(weather_zone_id)",
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_generation_type FOREIGN KEY (generation_type_id) REFERENCES dim_generation_type(generation_type_id)",
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_weather_condition FOREIGN KEY (weather_condition_id) REFERENCES dim_weather_condition(weather_condition_id)",
            "ALTER TABLE fact_energy_weather ADD CONSTRAINT FK_fact_socioeconomic_profile FOREIGN KEY (socioeconomic_profile_id) REFERENCES dim_socioeconomic_profile(socioeconomic_profile_id)"
        ]
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Utwórz tabelę
                cursor.execute(fact_ddl)
                
                # Dodaj foreign key constraints
                for constraint in fk_constraints:
                    cursor.execute(constraint)
                
                conn.commit()
                self.logger.info("Successfully created fact table with constraints")
                return True
                
        except Exception as e:
            self.logger.error(f"Error creating fact table: {str(e)}")
            return False
    
    def create_source_tables(self) -> bool:
        """
        Optymalizowane tworzenie tabel źródłowych
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating source tables (optimized)")
        
        source_ddl_statements = {
            'src_entso_actual_load': """
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
            """,
            'src_entso_generation': """
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
            """,
            'src_entso_forecast': """
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
            """,
            'src_weather_data': """
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
            """,
            'src_climate_data': """
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
            """,
            'src_eurostat_integrated': """
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
            """
        }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Wykonaj wszystkie DDL w jednej transakcji
                for table_name, ddl in source_ddl_statements.items():
                    self.logger.debug(f"Creating {table_name}")
                    cursor.execute(ddl)
                
                conn.commit()
                self.logger.info(f"Successfully created {len(source_ddl_statements)} source tables")
                return True
                
        except Exception as e:
            self.logger.error(f"Error creating source tables: {str(e)}")
            return False
    
    def insert_default_dimension_records(self) -> bool:
        """
        Optymalizowane wstawianie domyślnych rekordów jedną transakcją
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Inserting default dimension records (batch)")
        
        # Wszystkie default inserts w jednej liście
        default_inserts = [
            # dim_date
            """
            SET IDENTITY_INSERT dim_date ON;
            INSERT INTO dim_date (date_id, full_date, day_of_week, day_of_month, month, month_name, quarter, year, season, is_holiday, holiday_name, holiday_type, is_school_day, is_weekend)
            VALUES (0, '1900-01-01', 'Unknown', 0, 0, 'Unknown', 0, 0, 'Unknown', 'No', 'None', 'None', 'No', 'No');
            SET IDENTITY_INSERT dim_date OFF;
            """,
            # dim_time
            """
            SET IDENTITY_INSERT dim_time ON;
            INSERT INTO dim_time (time_id, hour, minute, day_period, is_peak_hour)
            VALUES (0, 0, 0, 'Unknown', 'No');
            SET IDENTITY_INSERT dim_time OFF;
            """,
            # dim_bidding_zone
            """
            SET IDENTITY_INSERT dim_bidding_zone ON;
            INSERT INTO dim_bidding_zone (bidding_zone_id, bidding_zone_code, bidding_zone_name, primary_country, secondary_countries, control_area, timezone, population, gdp_per_capita, energy_intensity, electricity_price_avg, year)
            VALUES (0, 'UNKNOWN', 'Unknown', 'UNK', 'None', 'Unknown', 'UTC', 0, 0.0, 0.0, 0.0, 0);
            SET IDENTITY_INSERT dim_bidding_zone OFF;
            """,
            # dim_weather_zone
            """
            SET IDENTITY_INSERT dim_weather_zone ON;
            INSERT INTO dim_weather_zone (weather_zone_id, weather_zone_name, bidding_zone_id, climate_zone, elevation_avg, coastal_proximity, urbanization_level)
            VALUES (0, 'Unknown', 0, 'Unknown', 0.0, 'Unknown', 'Unknown');
            SET IDENTITY_INSERT dim_weather_zone OFF;
            """,
            # dim_generation_type
            """
            SET IDENTITY_INSERT dim_generation_type ON;
            INSERT INTO dim_generation_type (generation_type_id, entso_code, generation_category, generation_type, is_intermittent, fuel_source)
            VALUES (0, 'B20', 'Unknown', 'Unknown', 'No', 'Unknown');
            SET IDENTITY_INSERT dim_generation_type OFF;
            """,
            # dim_weather_condition
            """
            SET IDENTITY_INSERT dim_weather_condition ON;
            INSERT INTO dim_weather_condition (weather_condition_id, condition_type, condition_severity, is_extreme_weather, extreme_weather_type)
            VALUES (0, 'Unknown', 'None', 'No', 'None');
            SET IDENTITY_INSERT dim_weather_condition OFF;
            """,
            # dim_socioeconomic_profile
            """
            SET IDENTITY_INSERT dim_socioeconomic_profile ON;
            INSERT INTO dim_socioeconomic_profile (socioeconomic_profile_id, bidding_zone_code, country_code, country_name, year, avg_income_level, unemployment_rate, urbanization_rate, service_sector_percentage, industry_sector_percentage, energy_poverty_rate, residential_percentage, commercial_percentage, industrial_percentage, avg_household_size, primary_heating_type, population)
            VALUES (0, 'UNKNOWN', 'UNK', 'Unknown', 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 'Unknown', 0);
            SET IDENTITY_INSERT dim_socioeconomic_profile OFF;
            """
        ]
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Wykonaj wszystkie inserts w jednej transakcji
                for insert_sql in default_inserts:
                    cursor.execute(insert_sql)
                
                conn.commit()
                self.logger.info(f"Successfully inserted {len(default_inserts)} default dimension records")
                return True
                
        except Exception as e:
            self.logger.error(f"Error inserting default dimension records: {str(e)}")
            return False
    
    def load_data_from_staging(self) -> bool:
        """
        Optymalizowane ładowanie danych z wykorzystaniem parallel processing
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Loading data from staging (parallel processing)")
        
        try:
            # Parallel loading of dimensions
            with ThreadPoolExecutor(max_workers=min(4, len(self.dimension_tables))) as executor:
                dimension_futures = {
                    executor.submit(self._load_dimension_data, dim): dim 
                    for dim in self.dimension_tables
                }
                
                dimension_results = []
                for future in as_completed(dimension_futures):
                    dim = dimension_futures[future]
                    try:
                        result = future.result()
                        dimension_results.append((dim, result))
                        if result:
                            self.logger.info(f"Successfully loaded dimension: {dim}")
                        else:
                            self.logger.warning(f"Failed to load dimension: {dim}")
                    except Exception as e:
                        self.logger.error(f"Error loading dimension {dim}: {str(e)}")
                        dimension_results.append((dim, False))
            
            # Parallel loading of source tables
            with ThreadPoolExecutor(max_workers=min(4, len(self.source_tables))) as executor:
                source_futures = {
                    executor.submit(self._load_source_data, src): src 
                    for src in self.source_tables
                }
                
                source_results = []
                for future in as_completed(source_futures):
                    src = source_futures[future]
                    try:
                        result = future.result()
                        source_results.append((src, result))
                        if result:
                            self.logger.info(f"Successfully loaded source: {src}")
                        else:
                            self.logger.warning(f"Failed to load source: {src}")
                    except Exception as e:
                        self.logger.error(f"Error loading source {src}: {str(e)}")
                        source_results.append((src, False))
            
            # Load fact data (sequential due to dependencies)
            fact_result = self._load_fact_data()
            
            # Evaluate results
            all_success = (
                all(result for _, result in dimension_results) and
                all(result for _, result in source_results) and
                fact_result
            )
            
            if all_success:
                self.logger.info("All data loaded successfully")
            else:
                self.logger.warning("Some data loading operations failed")
            
            return all_success
            
        except Exception as e:
            self.logger.error(f"Error in parallel data loading: {str(e)}")
            return False
    
    def _load_dimension_data(self, dim_table: str) -> bool:
        """
        Ładowanie danych wymiaru z optymalizacjami
        
        Args:
            dim_table: Nazwa tabeli wymiaru
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        staging_table = f"staging_{dim_table}"
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Sprawdź czy tabela staging istnieje i ma dane
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                        SELECT COUNT(*) FROM {staging_table}
                    ELSE
                        SELECT 0
                """)
                
                staging_count = cursor.fetchone()[0]
                
                if staging_count == 0:
                    self.logger.debug(f"No data in {staging_table}, skipping {dim_table}")
                    return True
                
                # Znajdź wspólne kolumny (używaj standardowego zapytania dla wymiarów)
                cursor.execute(self.sql_statements['get_common_columns'], (staging_table, dim_table, dim_table))
                common_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                if not common_columns:
                    self.logger.warning(f"No common columns found between {staging_table} and {dim_table}")
                    return False
                
                # Bulk insert with optimized SQL
                insert_sql = f"""
                INSERT INTO {dim_table} ({', '.join(common_columns)})
                SELECT {', '.join(common_columns)}
                FROM {staging_table}
                """
                
                cursor.execute(insert_sql)
                conn.commit()
                
                self.logger.debug(f"Loaded {staging_count} records to {dim_table}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error loading dimension {dim_table}: {str(e)}")
            return False
    
    def _load_source_data(self, src_table: str) -> bool:
        """
        Ładowanie danych źródłowych z optymalizacjami
        
        Args:
            src_table: Nazwa tabeli źródłowej
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        staging_table = f"staging_{src_table.replace('src_', '')}"
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Sprawdź czy tabela staging istnieje i ma dane
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                        SELECT COUNT(*) FROM {staging_table}
                    ELSE
                        SELECT 0
                """)
                
                staging_count = cursor.fetchone()[0]
                
                if staging_count == 0:
                    self.logger.debug(f"No data in {staging_table}, skipping {src_table}")
                    return True
                
                # Znajdź wspólne kolumny (używaj standardowego zapytania dla źródeł)
                cursor.execute(self.sql_statements['get_common_columns'], (staging_table, src_table, src_table))
                common_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                if not common_columns:
                    self.logger.warning(f"No common columns found between {staging_table} and {src_table}")
                    return False
                
                # Bulk insert
                insert_sql = f"""
                INSERT INTO {src_table} ({', '.join(common_columns)})
                SELECT {', '.join(common_columns)}
                FROM {staging_table}
                """
                
                cursor.execute(insert_sql)
                conn.commit()
                
                self.logger.debug(f"Loaded {staging_count} records to {src_table}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error loading source {src_table}: {str(e)}")
            return False
    
    def _load_fact_data(self) -> bool:
        """
        Optymalizowane ładowanie danych faktów z batch processing i proper NULL handling
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        staging_table = 'staging_fact_energy_weather'
        fact_table = 'fact_energy_weather'
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Sprawdź czy tabela staging istnieje i ma dane
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
                        SELECT COUNT(*) FROM {staging_table}
                    ELSE
                        SELECT 0
                """)
                
                staging_count = cursor.fetchone()[0]
                
                if staging_count == 0:
                    self.logger.debug(f"No data in {staging_table}, skipping {fact_table}")
                    return True
                
                # Znajdź wspólne kolumny (używaj fact-specific query)
                cursor.execute(self.sql_statements['get_common_columns_fact'], (staging_table, fact_table))
                common_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                
                if not common_columns:
                    self.logger.warning(f"No common columns found between {staging_table} and {fact_table}")
                    return False
                
                self.logger.info(f"Found {len(common_columns)} common columns for fact table")
                self.logger.debug(f"Common columns: {common_columns}")
                
                # Przygotuj kolumny z ISNULL dla kluczy obcych
                fk_columns = ['date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id', 
                             'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id']
                
                select_columns = []
                for col in common_columns:
                    if col in fk_columns:
                        select_columns.append(f"ISNULL({col}, 0) AS {col}")
                        self.logger.debug(f"Applied ISNULL to foreign key column: {col}")
                    else:
                        select_columns.append(col)
                
                # Bulk insert z batch processing dla dużych tabel
                if staging_count > self.batch_size:
                    self.logger.info(f"Large fact table detected ({staging_count} records), using batch processing")
                    
                    # Procesor batch
                    total_inserted = 0
                    offset = 0
                    batch_num = 0
                    start_time = time.time()  # Initialize timing
                    
                    while offset < staging_count:
                        batch_num += 1
                        current_batch_size = min(self.batch_size, staging_count - offset)
                        
                        self.logger.info(f"Processing batch {batch_num}: records {offset+1} to {offset+current_batch_size}")
                        
                        batch_sql = f"""
                        INSERT INTO {fact_table} ({', '.join(common_columns)})
                        SELECT {', '.join(select_columns)}
                        FROM {staging_table}
                        ORDER BY (SELECT NULL)
                        OFFSET {offset} ROWS
                        FETCH NEXT {self.batch_size} ROWS ONLY
                        """
                        
                        cursor.execute(batch_sql)
                        conn.commit()
                        
                        offset += self.batch_size
                        total_inserted += current_batch_size
                        
                        # Progress report every 5 batches for faster feedback
                        if batch_num % 5 == 0:
                            elapsed_time = time.time() - start_time
                            avg_time_per_batch = elapsed_time / batch_num
                            remaining_batches = max(0, (staging_count - offset) // self.batch_size)
                            estimated_remaining = remaining_batches * avg_time_per_batch
                            
                            self.logger.info(
                                f"Progress: {total_inserted}/{staging_count} records "
                                f"({(total_inserted/staging_count)*100:.1f}%) - "
                                f"Elapsed: {elapsed_time:.0f}s, "
                                f"Est. remaining: {estimated_remaining:.0f}s"
                            )
                    
                    self.logger.info(f"Successfully loaded {total_inserted} records to {fact_table} using batch processing")
                
                else:
                    # Pojedynczy insert dla mniejszych tabel
                    insert_sql = f"""
                    INSERT INTO {fact_table} ({', '.join(common_columns)})
                    SELECT {', '.join(select_columns)}
                    FROM {staging_table}
                    """
                    
                    cursor.execute(insert_sql)
                    conn.commit()
                    
                    self.logger.info(f"Successfully loaded {staging_count} records to {fact_table}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error loading fact data: {str(e)}")
            self.logger.error(f"This may be due to missing dimension records or data quality issues")
            
            # Try to provide more specific error information
            if "Cannot insert the value NULL" in str(e):
                self.logger.error("NULL value constraint violation detected")
                self.logger.error("Check if staging data has NULL values in required foreign key columns")
                self.logger.error("Ensure dimension tables are loaded first with default records")
            
            return False
    
    def run_full_rebuild(self) -> bool:
        """
        Optymalizowana pełna przebudowa hurtowni z miernikiem czasu
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        start_time = time.time()
        self.logger.info("Starting optimized full warehouse rebuild")
        
        steps = [
            ("Dropping existing tables", self.drop_tables),
            ("Creating dimension tables", self.create_dimension_tables),
            ("Inserting default dimension records", self.insert_default_dimension_records),
            ("Creating source tables", self.create_source_tables),
            ("Creating fact table", self.create_fact_table),
            ("Loading data from staging", self.load_data_from_staging)
        ]
        
        try:
            for step_name, step_func in steps:
                step_start = time.time()
                self.logger.info(f"Starting: {step_name}")
                
                if not step_func():
                    self.logger.error(f"Failed: {step_name}")
                    return False
                
                step_duration = time.time() - step_start
                self.logger.info(f"Completed: {step_name} in {step_duration:.2f} seconds")
            
            total_duration = time.time() - start_time
            self.logger.info(f"Optimized warehouse rebuild completed successfully in {total_duration:.2f} seconds")
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error during rebuild: {str(e)}")
            return False
        finally:
            # Cleanup connection pool
            self.close_connection_pool()

def main():
    """Główna funkcja z optymalizacjami - BEZ timeout dla długich operacji"""
    import sys
    import os
    
    # Konfiguracja logowania z lepszym formatem
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("optimized_rebuild_warehouse.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger("OptimizedWarehouseBuilderMain")
    
    logger.info("Starting Optimized Warehouse Rebuild - UNLIMITED EXECUTION TIME")
    
    # Parametry z zmiennych środowiskowych z wartościami domyślnymi
    connection_string = os.getenv('DW_CONNECTION_STRING', 
                                 "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=EnergyWeatherDW1;Trusted_Connection=yes;")
    max_connections = int(os.getenv('DW_MAX_CONNECTIONS', '3'))
    batch_size = int(os.getenv('DW_BATCH_SIZE', '5000'))
    
    logger.info(f"Configuration - Max connections: {max_connections}, Batch size: {batch_size}")
    logger.info("No timeouts configured - operations will run until completion")
    
    # Test połączenia przed rozpoczęciem (bez timeout)
    try:
        logger.info("Testing database connection...")
        test_conn = pyodbc.connect(connection_string)
        test_conn.timeout = 0  # Unlimited
        test_conn.close()
        logger.info("Database connection test successful")
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        logger.error("Please check your connection string and database availability")
        sys.exit(1)
    
    try:
        # Inicjalizacja OptimizedWarehouseBuilder
        builder = OptimizedWarehouseBuilder(
            connection_string=connection_string,
            max_connections=max_connections,
            batch_size=batch_size
        )
        
        # Uruchomienie pełnej przebudowy
        success = builder.run_full_rebuild()
        
        if success:
            logger.info("Optimized warehouse rebuild completed successfully")
            sys.exit(0)
        else:
            logger.error("Optimized warehouse rebuild failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Rebuild interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()