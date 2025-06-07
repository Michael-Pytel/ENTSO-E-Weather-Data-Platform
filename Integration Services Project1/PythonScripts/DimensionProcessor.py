"""
DimensionProcessor.py
Przetwarzanie danych dla tabel wymiarowych
Generowanie wymiarów daty, czasu, mapowanie kodów stref, itp.
Zaktualizowana wersja z obsługą SCD Type 2
"""

import pandas as pd
from datetime import datetime, timedelta, date
import pyodbc
import logging
from typing import Dict, List, Optional, Tuple
import calendar
import hashlib
import json

class DimensionProcessor:
    """Procesor do tworzenia i aktualizacji wymiarów hurtowni danych z obsługą SCD Type 2"""
    
    def __init__(self, connection_string: str):
        """
        Inicjalizacja procesora wymiarów
        
        Args:
            connection_string: String połączenia z bazą danych
        """
        self.connection_string = connection_string
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Mapowanie stref przetargowych ENTSO-E
        self.bidding_zone_mapping = {
            '10YPL-AREA-----S': {
                'name': 'Poland',
                'country': 'PL',
                'timezone': 'Europe/Warsaw',
                'control_area': 'PSE'
            },
            '10YDE-EON------1': {
                'name': 'Germany',
                'country': 'DE', 
                'timezone': 'Europe/Berlin',
                'control_area': 'TenneT'
            },
            '10YFR-RTE------C': {
                'name': 'France',
                'country': 'FR',
                'timezone': 'Europe/Paris',
                'control_area': 'RTE'
            },
            '10YES-REE------0': {
                'name': 'Spain',
                'country': 'ES',
                'timezone': 'Europe/Madrid',
                'control_area': 'REE'
            },
            '10YIT-GRTN-----B': {
                'name': 'Italy',
                'country': 'IT',
                'timezone': 'Europe/Rome',
                'control_area': 'Terna'
            },
            # Dodajemy więcej krajów UE
            '10YCZ-CEPS-----N': {
                'name': 'Czech Republic',
                'country': 'CZ',
                'timezone': 'Europe/Prague',
                'control_area': 'CEPS'
            },
            '10YSK-SEPS-----K': {
                'name': 'Slovakia',
                'country': 'SK',
                'timezone': 'Europe/Bratislava',
                'control_area': 'SEPS'
            },
            '10YAT-APG------L': {
                'name': 'Austria',
                'country': 'AT',
                'timezone': 'Europe/Vienna',
                'control_area': 'APG'
            },
            '10YHU-MAVIR----U': {
                'name': 'Hungary',
                'country': 'HU',
                'timezone': 'Europe/Budapest',
                'control_area': 'MAVIR'
            },
            '10YNL----------L': {
                'name': 'Netherlands',
                'country': 'NL',
                'timezone': 'Europe/Amsterdam',
                'control_area': 'TenneT NL'
            },
            '10YBE----------2': {
                'name': 'Belgium',
                'country': 'BE',
                'timezone': 'Europe/Brussels',
                'control_area': 'Elia'
            },
            '10YLU-CEGEDEL-NQ': {
                'name': 'Luxembourg',
                'country': 'LU',
                'timezone': 'Europe/Luxembourg',
                'control_area': 'Creos'
            },
            '10YIE-1001A00010': {
                'name': 'Ireland',
                'country': 'IE',
                'timezone': 'Europe/Dublin',
                'control_area': 'EirGrid'
            },
            '10YDK-1-------W': {
                'name': 'Denmark',
                'country': 'DK',
                'timezone': 'Europe/Copenhagen',
                'control_area': 'Energinet'
            },
            '10YSE-1--------K': {
                'name': 'Sweden',
                'country': 'SE',
                'timezone': 'Europe/Stockholm',
                'control_area': 'Svenska Kraftnät'
            },
            '10YFI-1--------U': {
                'name': 'Finland',
                'country': 'FI',
                'timezone': 'Europe/Helsinki',
                'control_area': 'Fingrid'
            },
            '10YPT-REN------W': {
                'name': 'Portugal',
                'country': 'PT',
                'timezone': 'Europe/Lisbon',
                'control_area': 'REN'
            },
            '10YGR-HTSO-----Y': {
                'name': 'Greece',
                'country': 'GR',
                'timezone': 'Europe/Athens',
                'control_area': 'IPTO'
            },
            '10YRO-TEL------P': {
                'name': 'Romania',
                'country': 'RO',
                'timezone': 'Europe/Bucharest',
                'control_area': 'Transelectrica'
            },
            '10YBG-ESO------M': {
                'name': 'Bulgaria',
                'country': 'BG',
                'timezone': 'Europe/Sofia',
                'control_area': 'ESO'
            },
            '10YHR-HEP------M': {
                'name': 'Croatia',
                'country': 'HR',
                'timezone': 'Europe/Zagreb',
                'control_area': 'HOPS'
            },
            '10YSI-ELES-----O': {
                'name': 'Slovenia',
                'country': 'SI',
                'timezone': 'Europe/Ljubljana',
                'control_area': 'ELES'
            },
            '10YLT-1001A0008Q': {
                'name': 'Lithuania',
                'country': 'LT',
                'timezone': 'Europe/Vilnius',
                'control_area': 'Litgrid'
            },
            '10YLV-1001A00074': {
                'name': 'Latvia',
                'country': 'LV',
                'timezone': 'Europe/Riga',
                'control_area': 'AST'
            },
            '10YEE-1001A0016J': {
                'name': 'Estonia',
                'country': 'EE',
                'timezone': 'Europe/Tallinn',
                'control_area': 'Elering'
            }
        }
        
        # Mapowanie typów generacji energii
        self.generation_type_mapping = {
            'B01': {
                'type': 'Biomass',
                'category': 'Renewable',
                'is_intermittent': False,
                'fuel_source': 'Biomass'
            },
            'B02': {
                'type': 'Fossil Brown coal/Lignite',
                'category': 'Conventional',
                'is_intermittent': False,
                'fuel_source': 'Coal'
            },
            'B03': {
                'type': 'Fossil Coal-derived gas',
                'category': 'Conventional',
                'is_intermittent': False,
                'fuel_source': 'Gas'
            },
            'B04': {
                'type': 'Fossil Gas',
                'category': 'Conventional',
                'is_intermittent': False,
                'fuel_source': 'Natural Gas'
            },
            'B05': {
                'type': 'Fossil Hard coal',
                'category': 'Conventional',
                'is_intermittent': False,
                'fuel_source': 'Coal'
            },
            'B06': {
                'type': 'Fossil Oil',
                'category': 'Conventional',
                'is_intermittent': False,
                'fuel_source': 'Oil'
            },
            'B09': {
                'type': 'Geothermal',
                'category': 'Renewable',
                'is_intermittent': False,
                'fuel_source': 'Geothermal'
            },
            'B10': {
                'type': 'Hydro Pumped Storage',
                'category': 'Renewable',
                'is_intermittent': False,
                'fuel_source': 'Hydro'
            },
            'B11': {
                'type': 'Hydro Run-of-river',
                'category': 'Renewable',
                'is_intermittent': True,
                'fuel_source': 'Hydro'
            },
            'B12': {
                'type': 'Hydro Water Reservoir',
                'category': 'Renewable',
                'is_intermittent': False,
                'fuel_source': 'Hydro'
            },
            'B13': {
                'type': 'Marine',
                'category': 'Renewable',
                'is_intermittent': True,
                'fuel_source': 'Marine'
            },
            'B14': {
                'type': 'Nuclear',
                'category': 'Nuclear',
                'is_intermittent': False,
                'fuel_source': 'Nuclear'
            },
            'B15': {
                'type': 'Other renewable',
                'category': 'Renewable',
                'is_intermittent': True,
                'fuel_source': 'Other'
            },
            'B16': {
                'type': 'Solar',
                'category': 'Renewable',
                'is_intermittent': True,
                'fuel_source': 'Solar'
            },
            'B17': {
                'type': 'Waste',
                'category': 'Other',
                'is_intermittent': False,
                'fuel_source': 'Waste'
            },
            'B18': {
                'type': 'Wind Offshore',
                'category': 'Renewable',
                'is_intermittent': True,
                'fuel_source': 'Wind'
            },
            'B19': {
                'type': 'Wind Onshore',
                'category': 'Renewable',
                'is_intermittent': True,
                'fuel_source': 'Wind'
            },
            'B20': {
                'type': 'Other',
                'category': 'Other',
                'is_intermittent': False,
                'fuel_source': 'Other'
            }
        }
        
        # Konfiguracja warunków pogodowych
        self.weather_conditions = [
            {'type': 'Clear', 'severity': 'Mild', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Partly Cloudy', 'severity': 'Mild', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Cloudy', 'severity': 'Mild', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Overcast', 'severity': 'Mild', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Rain', 'severity': 'Moderate', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Heavy Rain', 'severity': 'Severe', 'is_extreme': True, 'extreme_type': 'Precipitation'},
            {'type': 'Snow', 'severity': 'Moderate', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Heavy Snow', 'severity': 'Severe', 'is_extreme': True, 'extreme_type': 'Precipitation'},
            {'type': 'Windy', 'severity': 'Moderate', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Storm', 'severity': 'Severe', 'is_extreme': True, 'extreme_type': 'Wind'},
            {'type': 'Fog', 'severity': 'Mild', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Heatwave', 'severity': 'Severe', 'is_extreme': True, 'extreme_type': 'Temperature'},
            {'type': 'Frost', 'severity': 'Moderate', 'is_extreme': False, 'extreme_type': 'None'},
            {'type': 'Extreme Cold', 'severity': 'Severe', 'is_extreme': True, 'extreme_type': 'Temperature'}
        ]
        
        # Definicje wymiarów SCD Type 2
        self.scd2_dimensions = [
            'dim_bidding_zone',
            'dim_socioeconomic_profile',
            'dim_weather_zone'
        ]
    def save_dimensions_to_staging(self, dimensions: Dict[str, pd.DataFrame]) -> bool:
        """
        Zapisanie wymiarów do tabel staging
        
        Args:
            dimensions: Słownik z DataFrame'ami wymiarów
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            
            for dim_name, df in dimensions.items():
                if df.empty:
                    continue
                
                staging_table_name = f"staging_{dim_name}"
                
                # Utworzenie tabeli staging
                self._create_dimension_staging_table(conn, staging_table_name, df)
                
                # Czyszczenie tabeli staging
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {staging_table_name}")
                
                # Wstawianie danych
                self._bulk_insert_dimension_data(conn, staging_table_name, df)
                
                self.logger.info(f"Saved {len(df)} records to {staging_table_name}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving dimensions to staging: {str(e)}")
            return False
    
    def _create_dimension_staging_table(self, conn, table_name: str, df: pd.DataFrame):
        """
        Utworzenie tabeli staging dla wymiaru z odpowiednimi rozmiarami kolumn
        
        Args:
            conn: Połączenie z bazą danych
            table_name: Nazwa tabeli staging
            df: DataFrame z danymi
        """
        cursor = conn.cursor()
        
        # Mapowanie typów kolumn
        column_definitions = []
        
        # Minimalne długości dla kolumn tekstowych - zabezpieczenie przed obcięciem
        min_text_lengths = {
            'holiday_name': 100,
            'month_name': 20,
            'day_of_week': 20,
            'season': 20,
            'holiday_type': 50,
            'country_name': 100,
            'bidding_zone_name': 100,
            'weather_zone_name': 100,
            'timezone': 50,
            'control_area': 50,
            'condition_type': 50,
            'condition_severity': 30,
            'extreme_weather_type': 50,
            'climate_zone': 50,
            'coastal_proximity': 30,
            'urbanization_level': 30,
            'primary_heating_type': 50,
            'profile_name': 100
        }
        
        for column in df.columns:
            sample_value = df[column].dropna().iloc[0] if not df[column].dropna().empty else None
            
            if pd.api.types.is_integer_dtype(df[column]):
                if df[column].max() < 128:
                    col_type = "TINYINT"
                elif df[column].max() < 32768:
                    col_type = "SMALLINT"
                elif df[column].max() < 2147483648:
                    col_type = "INT"
                else:
                    col_type = "BIGINT"
            elif pd.api.types.is_float_dtype(df[column]):
                col_type = "DECIMAL(15,2)"
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                col_type = "DATETIME2"
            elif isinstance(sample_value, date):
                col_type = "DATE"
            else:
                # Dla pól tekstowych
                max_length = df[column].astype(str).str.len().max() if not df[column].empty else 50
                
                # Uwzględnij minimalne długości dla znanych kolumn
                if column in min_text_lengths:
                    max_length = max(max_length, min_text_lengths[column])
                else:
                    max_length = max(max_length, 50)  # Minimum 50 znaków dla innych kolumn
                    
                max_length = min(max_length, 500)  # Ograniczenie do 500 znaków
                col_type = f"NVARCHAR({max_length})"
            
            column_definitions.append(f"{column} {col_type}")
        
        column_definitions.append("created_at DATETIME2 DEFAULT GETDATE()")
        
        # Sprawdź czy tabela już istnieje
        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                DROP TABLE {table_name}
        """)
        conn.commit()
        
        # Utwórz tabelę od nowa
        create_sql = f"""
        CREATE TABLE {table_name} (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            {', '.join(column_definitions)}
        )
        """
        
        cursor.execute(create_sql)
        conn.commit()
        
        self.logger.info(f"Created staging table: {table_name}")
    
    def _bulk_insert_dimension_data(self, conn, table_name: str, df: pd.DataFrame):
        """Bulk insert danych wymiaru"""
        cursor = conn.cursor()
        
        columns = list(df.columns)
        
        for _, row in df.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if pd.isna(value):
                    values.append(None)
                elif isinstance(value, (date, datetime)):
                    # Konwersja dat na stringi w formacie ISO, które SQL Server może zrozumieć
                    values.append(value.isoformat())
                else:
                    values.append(value)
            
            placeholders = ', '.join(['?' for _ in values])
            insert_sql = f"""
                INSERT INTO {table_name} 
                ({', '.join(columns)}) 
                VALUES ({placeholders})
            """
            
            try:
                cursor.execute(insert_sql, values)
            except Exception as e:
                self.logger.error(f"Error inserting row into {table_name}: {str(e)}")
                self.logger.error(f"SQL: {insert_sql}")
                self.logger.error(f"Values: {values}")
                raise
    
    def update_dimensions_with_scd2(self) -> bool:
        """
        Aktualizacja wymiarów docelowych z tabel staging z obsługą SCD Type 2
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Updating dimensions with SCD Type 2")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for dim_name in self.scd2_dimensions:
                staging_table = f"staging_{dim_name}"
                target_table = dim_name
                
                # Sprawdź czy tabele istnieją
                cursor.execute(f"""
                    IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL 
                        AND OBJECT_ID('{target_table}', 'U') IS NOT NULL
                        SELECT 1
                    ELSE
                        SELECT 0
                """)
                
                tables_exist = cursor.fetchone()[0]
                if not tables_exist:
                    self.logger.warning(f"Tables {staging_table} or {target_table} do not exist. Skipping SCD2 update.")
                    continue
                
                # Przygotuj procedurę SCD2 dla tego wymiaru
                self._prepare_scd2_procedure(conn, dim_name)
                
                # Wywołaj procedurę SCD2
                self.logger.info(f"Executing SCD2 update for {dim_name}")
                cursor.execute(f"EXEC usp_scd2_update_{dim_name}")
                
                # Zlicz zmiany
                cursor.execute(f"""
                    SELECT 
                        SUM(CASE WHEN change_type = 'INSERT' THEN 1 ELSE 0 END) as inserts,
                        SUM(CASE WHEN change_type = 'UPDATE' THEN 1 ELSE 0 END) as updates
                    FROM 
                        scd2_change_log
                    WHERE 
                        dimension_name = '{dim_name}'
                        AND change_date > DATEADD(hour, -1, GETDATE())
                """)
                
                result = cursor.fetchone()
                if result:
                    inserts, updates = result.inserts or 0, result.updates or 0
                    self.logger.info(f"SCD2 update for {dim_name}: {inserts} inserts, {updates} updates")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating dimensions with SCD2: {str(e)}")
            return False
    
    def _prepare_scd2_procedure(self, conn, dimension_name: str):
        """
        Przygotowuje procedurę SQL dla obsługi SCD Type 2 dla danego wymiaru
        
        Args:
            conn: Połączenie z bazą danych
            dimension_name: Nazwa wymiaru
        """
        cursor = conn.cursor()
        
        # Utwórz tabelę logowania zmian SCD2 jeśli nie istnieje
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'scd2_change_log')
            CREATE TABLE scd2_change_log (
                log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                dimension_name NVARCHAR(100) NOT NULL,
                business_key NVARCHAR(100) NOT NULL,
                change_type NVARCHAR(20) NOT NULL,
                change_date DATETIME2 DEFAULT GETDATE(),
                old_hash NVARCHAR(32),
                new_hash NVARCHAR(32)
            )
        """)
        
        # Pobierz schemat tabeli docelowej
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{dimension_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        if not columns:
            # Jeśli tabela docelowa nie istnieje, utwórz ją na podstawie tabeli staging
            staging_table = f"staging_{dimension_name}"
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{staging_table}'
                ORDER BY ORDINAL_POSITION
            """)
            columns = cursor.fetchall()
            
            if columns:
                # Tworzenie tabeli docelowej z dodatkowymi kolumnami SCD2
                column_defs = []
                for col in columns:
                    col_name = col.COLUMN_NAME
                    data_type = col.DATA_TYPE
                    max_length = col.CHARACTER_MAXIMUM_LENGTH
                    
                    # Pomiń kolumnę id z tabeli staging
                    if col_name.lower() == 'id':
                        continue
                    
                    if max_length == -1:  # nvarchar(max)
                        column_defs.append(f"{col_name} {data_type}(max)")
                    elif max_length is not None:
                        column_defs.append(f"{col_name} {data_type}({max_length})")
                    else:
                        column_defs.append(f"{col_name} {data_type}")
                
                # Dodaj kolumny SCD2 jeśli nie zostały jeszcze dodane
                if not any(col.COLUMN_NAME == 'valid_from' for col in columns):
                    column_defs.append("valid_from DATE NOT NULL")
                    column_defs.append("valid_to DATE NOT NULL")
                    column_defs.append("is_current NVARCHAR(3) NOT NULL")
                    column_defs.append("business_key NVARCHAR(100) NOT NULL")
                    column_defs.append("row_hash NVARCHAR(32) NOT NULL")
                
                # Utwórz tabelę docelową
                create_table_sql = f"""
                CREATE TABLE {dimension_name} (
                    {dimension_name}_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    {', '.join(column_defs)},
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2 DEFAULT GETDATE()
                )
                """
                
                cursor.execute(create_table_sql)
                conn.commit()
        
        # Utwórz procedurę aktualizacji SCD2
        procedure_name = f"usp_scd2_update_{dimension_name}"
        
        # Sprawdź czy procedura już istnieje
        cursor.execute(f"""
            IF OBJECT_ID('{procedure_name}', 'P') IS NOT NULL
                DROP PROCEDURE {procedure_name}
        """)
        
        # Określ kolumny biznesowe (wszystkie oprócz technicznych)
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{dimension_name}'
            AND COLUMN_NAME NOT IN (
                '{dimension_name}_id', 'valid_from', 'valid_to', 'is_current', 
                'business_key', 'row_hash', 'created_at', 'updated_at'
            )
        """)
        
        business_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
        business_columns_str = ', '.join(business_columns)
        
        # Utwórz procedurę
        create_procedure_sql = f"""
        CREATE PROCEDURE {procedure_name}
        AS
        BEGIN
            SET NOCOUNT ON;
            
            DECLARE @CurrentDate DATE = GETDATE();
            
            -- Tymczasowa tabela do śledzenia zmian
            CREATE TABLE #Changes (
                business_key NVARCHAR(100),
                change_type NVARCHAR(10),
                old_hash NVARCHAR(32),
                new_hash NVARCHAR(32)
            );
            
            -- 1. Identyfikuj nowe rekordy (INSERT)
            INSERT INTO #Changes (business_key, change_type, old_hash, new_hash)
            SELECT 
                s.business_key,
                'INSERT',
                NULL,
                s.row_hash
            FROM 
                staging_{dimension_name} s
            WHERE 
                NOT EXISTS (
                    SELECT 1 FROM {dimension_name} t 
                    WHERE t.business_key = s.business_key
                    AND t.is_current = 'Yes'
                );
            
            -- 2. Identyfikuj zmienione rekordy (UPDATE)
            INSERT INTO #Changes (business_key, change_type, old_hash, new_hash)
            SELECT 
                s.business_key,
                'UPDATE',
                t.row_hash,
                s.row_hash
            FROM 
                staging_{dimension_name} s
            INNER JOIN 
                {dimension_name} t ON s.business_key = t.business_key AND t.is_current = 'Yes'
            WHERE 
                s.row_hash <> t.row_hash;
            
            -- 3. Zamknij stare wersje rekordów (dla UPDATE)
            UPDATE t
            SET 
                valid_to = @CurrentDate,
                is_current = 'No',
                updated_at = GETDATE()
            FROM 
                {dimension_name} t
            INNER JOIN 
                #Changes c ON t.business_key = c.business_key
            WHERE 
                c.change_type = 'UPDATE'
                AND t.is_current = 'Yes';
            
            -- 4. Wstaw nowe wersje (dla INSERT i UPDATE)
            INSERT INTO {dimension_name} (
                {business_columns_str},
                valid_from, valid_to, is_current, business_key, row_hash
            )
            SELECT 
                {business_columns_str},
                @CurrentDate, 
                '9999-12-31', 
                'Yes',
                s.business_key,
                s.row_hash
            FROM 
                staging_{dimension_name} s
            INNER JOIN 
                #Changes c ON s.business_key = c.business_key;
            
            -- 5. Zapisz zmiany w logu
            INSERT INTO scd2_change_log (
                dimension_name, business_key, change_type, old_hash, new_hash
            )
            SELECT 
                '{dimension_name}',
                business_key,
                change_type,
                old_hash,
                new_hash
            FROM 
                #Changes;
            
            -- 6. Zwróć podsumowanie zmian
            SELECT 
                SUM(CASE WHEN change_type = 'INSERT' THEN 1 ELSE 0 END) as inserts,
                SUM(CASE WHEN change_type = 'UPDATE' THEN 1 ELSE 0 END) as updates
            FROM 
                #Changes;
            
            DROP TABLE #Changes;
        END
        """
        
        cursor.execute(create_procedure_sql)
        conn.commit()
    
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
                # Jeśli procedura nie istnieje, dodaj wpis do tabeli logowania bezpośrednio
                self.logger.warning("Stored procedure sp_log_etl_process not found, inserting log directly")
                
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_log')
                    CREATE TABLE etl_log (
                        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        process_name NVARCHAR(100) NOT NULL,
                        start_time DATETIME2 NOT NULL,
                        end_time DATETIME2,
                        status NVARCHAR(20) NOT NULL,
                        records_processed INT DEFAULT 0,
                        error_message NVARCHAR(MAX),
                        execution_details NVARCHAR(MAX)
                    )
                """)
                
                if status == 'RUNNING':
                    cursor.execute("""
                        INSERT INTO etl_log (process_name, start_time, status, records_processed, error_message)
                        VALUES (?, GETDATE(), ?, ?, ?)
                    """, (process_name, status, records, error_msg))
                else:
                    cursor.execute("""
                        UPDATE etl_log 
                        SET end_time = GETDATE(), 
                            status = ?, 
                            records_processed = ?,
                            error_message = ?
                        WHERE process_name = ? 
                          AND status = 'RUNNING'
                          AND log_id = (SELECT MAX(log_id) FROM etl_log WHERE process_name = ? AND status = 'RUNNING')
                    """, (status, records, error_msg, process_name, process_name))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging process: {str(e)}")

    
    def generate_date_dimension(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Generowanie wymiaru daty
        
        Args:
            start_date: Data początkowa
            end_date: Data końcowa
            
        Returns:
            DataFrame z wymiarem daty
        """
        self.logger.info(f"Generating date dimension from {start_date} to {end_date}")
        
        date_list = []
        current_date = start_date
        
        # Uproszczona wersja bez korzystania z pakietu holidays
        # Zamiast tego, używamy prostej listy świąt dla głównych krajów
        simple_holidays = {
            # Polski (dni wolne od pracy)
            "PL": [
                (1, 1, "Nowy Rok"),
                (6, 1, "Święto Trzech Króli"),
                (5, 1, "Święto Pracy"),
                (3, 5, "Święto Konstytucji 3 Maja"),
                (15, 8, "Wniebowzięcie Najświętszej Maryi Panny"),
                (1, 11, "Wszystkich Świętych"),
                (11, 11, "Święto Niepodległości"),
                (25, 12, "Boże Narodzenie"),
                (26, 12, "Drugi dzień Bożego Narodzenia")
                # Nie dodajemy Wielkanocy, bo to święto ruchome
            ],
            # Niemcy (podstawowe święta)
            "DE": [
                (1, 1, "Neujahrstag"),
                (1, 5, "Tag der Arbeit"),
                (3, 10, "Tag der Deutschen Einheit"),
                (25, 12, "Weihnachtstag"),
                (26, 12, "Zweiter Weihnachtstag")
            ],
            # Francja (podstawowe święta)
            "FR": [
                (1, 1, "Jour de l'an"),
                (1, 5, "Fête du Travail"),
                (8, 5, "Fête de la Victoire"),
                (14, 7, "Fête nationale"),
                (15, 8, "Assomption"),
                (1, 11, "Toussaint"),
                (11, 11, "Armistice"),
                (25, 12, "Noël")
            ]
        }
        
        while current_date <= end_date:
            # Podstawowe atrybuty daty
            weekday = current_date.weekday()
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            
            # Określenie pory roku
            month = current_date.month
            if month in [12, 1, 2]:
                season = 'Winter'
            elif month in [3, 4, 5]:
                season = 'Spring'
            elif month in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Autumn'
            
            # Sprawdzenie czy to święto (uproszczone)
            is_holiday = 'No'
            holiday_name = ''
            holiday_type = ''
            
            for country, holidays_list in simple_holidays.items():
                for holiday_day, holiday_month, name in holidays_list:
                    if current_date.day == holiday_day and current_date.month == holiday_month:
                        is_holiday = 'Yes'
                        holiday_name = name
                        holiday_type = 'National'
                        break
                if is_holiday == 'Yes':
                    break
            
            # Określenie czy to dzień szkolny (nie weekend i nie święto)
            is_school_day = 'Yes' if weekday < 5 and is_holiday == 'No' else 'No'
            
            date_record = {
                'full_date': current_date,
                'day_of_week': day_names[weekday],
                'day_of_month': current_date.day,
                'month': current_date.month,
                'month_name': month_names[current_date.month - 1],
                'quarter': (current_date.month - 1) // 3 + 1,
                'year': current_date.year,
                'season': season,
                'is_holiday': is_holiday,
                'holiday_name': holiday_name,
                'holiday_type': holiday_type,
                'is_school_day': is_school_day,
                'is_weekend': 'Yes' if weekday >= 5 else 'No'
            }
            
            date_list.append(date_record)
            current_date += timedelta(days=1)
        
        return pd.DataFrame(date_list)
    
    def generate_time_dimension(self) -> pd.DataFrame:
        """
        Generowanie wymiaru czasu (godziny w dobie)
        
        Returns:
            DataFrame z wymiarem czasu
        """
        self.logger.info("Generating time dimension")
        
        time_list = []
        
        for hour in range(24):
            for minute in [0]:  # Tylko pełne godziny
                # Określenie pory dnia
                if 6 <= hour < 12:
                    day_period = 'Morning'
                elif 12 <= hour < 18:
                    day_period = 'Afternoon'
                elif 18 <= hour < 22:
                    day_period = 'Evening'
                else:
                    day_period = 'Night'
                
                # Określenie czy to godzina szczytu (7-9 i 17-20)
                is_peak_hour = 'Yes' if (7 <= hour <= 9) or (17 <= hour <= 20) else 'No'
                
                time_record = {
                    'hour': hour,
                    'minute': minute,
                    'day_period': day_period,
                    'is_peak_hour': is_peak_hour
                }
                
                time_list.append(time_record)
        
        return pd.DataFrame(time_list)
    
    def load_socioeconomic_data_from_staging(self) -> pd.DataFrame:
        """
        Wczytuje dane socjoekonomiczne z tabeli staging dla wszystkich krajów UE,
        włącznie z danymi historycznymi
        
        Returns:
            DataFrame z danymi socjoekonomicznymi lub pusty DataFrame w przypadku błędu
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            query = """
                SELECT 
                    country_code, country_name, year, population, gdp_per_capita,
                    electricity_price_avg, energy_intensity, unemployment_rate,
                    poverty_by_degree_of_urbanization, service_sector_percentage,
                    industry_sector_percentage, avg_household_size, energy_poverty_rate,
                    primary_heating_type, data_quality_score
                FROM 
                    staging_eurostat_integrated
                ORDER BY 
                    country_code, year DESC
            """
            
            socioeconomic_data = pd.read_sql(query, conn)
            conn.close()
            
            self.logger.info(f"Loaded {len(socioeconomic_data)} socioeconomic records from staging")
            return socioeconomic_data
            
        except Exception as e:
            self.logger.error(f"Error loading socioeconomic data from staging: {str(e)}")
            return pd.DataFrame()
    
    def process_bidding_zone_dimension(self, socioeconomic_data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Przetwarzanie wymiaru stref przetargowych z danymi socjoekonomicznymi
        z uwzględnieniem danych historycznych podobnie jak w wymiarze socioeconomic_profile
        
        Args:
            socioeconomic_data: DataFrame z danymi socjoekonomicznymi z Eurostat
                
        Returns:
            DataFrame z wymiarem stref przetargowych
        """
        self.logger.info("Processing bidding zone dimension")
        
        # Jeśli nie dostarczono danych socjoekonomicznych, spróbuj je wczytać ze staging
        if socioeconomic_data is None or socioeconomic_data.empty:
            socioeconomic_data = self.load_socioeconomic_data_from_staging()
        
        zones_list = []
        
        # Dla każdej strefy przetargowej
        for zone_code, zone_info in self.bidding_zone_mapping.items():
            country_code = zone_info['country']
            
            # Pobierz dane socjoekonomiczne dla kraju
            country_data = socioeconomic_data[socioeconomic_data['country_code'] == country_code]
            
            if country_data.empty:
                # Jeśli brak danych dla kraju, dodaj jeden rekord bez danych historycznych
                zone_record = {
                    'bidding_zone_code': zone_code,
                    'bidding_zone_name': zone_info['name'],
                    'primary_country': country_code,
                    'secondary_countries': 'None',
                    'control_area': zone_info['control_area'],
                    'timezone': zone_info['timezone'],
                    'population': 0,
                    'gdp_per_capita': 0.0,
                    'energy_intensity': 0.0,
                    'electricity_price_avg': 0.0,
                    # Dodatkowe pola dla SCD Type 2
                    'valid_from': date(2020, 1, 1),  # Domyślna data początku
                    'valid_to': date(9999, 12, 31),  # Domyślna data końca
                    'is_current': 'Yes'
                }
                
                # Generowanie business key i hash dla SCD Type 2
                zone_record['business_key'] = zone_code
                zone_record['row_hash'] = self._generate_row_hash(zone_record, 
                    ['bidding_zone_code', 'population', 'gdp_per_capita', 'energy_intensity', 'electricity_price_avg'])
                
                zones_list.append(zone_record)
            else:
                # Posortuj dane wg roku (od najstarszych)
                country_data = country_data.sort_values('year')
                
                # Przetwarzamy każdy rok
                for i, (_, data) in enumerate(country_data.iterrows()):
                    year = int(data['year'])
                    
                    # Ustal okres obowiązywania:
                    # - Dane z roku X obowiązują od 1 stycznia roku X+1
                    # - Jeśli to ostatni rekord dla kraju, obowiązuje do 31.12.9999
                    # - W przeciwnym razie obowiązuje do końca roku (31 grudnia)
                    valid_from = date(year + 1, 1, 1)
                    
                    if i < len(country_data) - 1:
                        next_year = int(country_data.iloc[i + 1]['year'])
                        valid_to = date(next_year + 1, 1, 1) - timedelta(days=1)
                        is_current = 'No'
                    else:
                        valid_to = date(9999, 12, 31)
                        is_current = 'Yes'
                    
                    zone_record = {
                        'bidding_zone_code': zone_code,
                        'bidding_zone_name': zone_info['name'],
                        'primary_country': country_code,
                        'secondary_countries': 'None',
                        'control_area': zone_info['control_area'],
                        'timezone': zone_info['timezone'],
                        'population': data.get('population', 0) or 0,
                        'gdp_per_capita': data.get('gdp_per_capita', 0.0) or 0.0,
                        'energy_intensity': data.get('energy_intensity', 0.0) or 0.0,
                        'electricity_price_avg': data.get('electricity_price_avg', 0.0) or 0.0,
                        # Dodatkowe pola dla SCD Type 2
                        'valid_from': valid_from,
                        'valid_to': valid_to,
                        'is_current': is_current,
                        # Dodajemy rok do identyfikacji wersji
                        'year': year
                    }
                    
                    # Generowanie business key i hash dla SCD Type 2
                    zone_record['business_key'] = f"{zone_code}_{year}"
                    zone_record['row_hash'] = self._generate_row_hash(zone_record, 
                        ['bidding_zone_code', 'year', 'population', 'gdp_per_capita', 'energy_intensity', 'electricity_price_avg'])
                    
                    zones_list.append(zone_record)
        
        return pd.DataFrame(zones_list)
    
    def generate_generation_type_dimension(self) -> pd.DataFrame:
        """
        Generowanie wymiaru typów generacji energii
        
        Returns:
            DataFrame z wymiarem typów generacji
        """
        self.logger.info("Generating generation type dimension")
        
        generation_list = []
        
        for code, info in self.generation_type_mapping.items():
            generation_record = {
                'entso_code': code,
                'generation_category': info['category'],
                'generation_type': info['type'],
                'is_intermittent': 'Yes' if info['is_intermittent'] else 'No',
                'fuel_source': info['fuel_source']
            }
            
            generation_list.append(generation_record)
        
        return pd.DataFrame(generation_list)
    
    def generate_weather_condition_dimension(self) -> pd.DataFrame:
        """
        Generowanie wymiaru warunków pogodowych
        
        Returns:
            DataFrame z wymiarem warunków pogodowych
        """
        self.logger.info("Generating weather condition dimension")
        
        condition_list = []
        
        for condition in self.weather_conditions:
            condition_record = {
                'condition_type': condition['type'],
                'condition_severity': condition['severity'],
                'is_extreme_weather': 'Yes' if condition['is_extreme'] else 'No',
                'extreme_weather_type': condition['extreme_type']
            }
            
            condition_list.append(condition_record)
        
        return pd.DataFrame(condition_list)
    
    def generate_weather_zone_dimension(self, bidding_zones: pd.DataFrame) -> pd.DataFrame:
        """
        Generowanie wymiaru stref pogodowych
        
        Args:
            bidding_zones: DataFrame ze strefami przetargowymi
            
        Returns:
            DataFrame z wymiarem stref pogodowych
        """
        self.logger.info("Generating weather zone dimension")
        
        weather_zones_list = []
        current_date = datetime.now().date()
        
        # Mapowanie stref pogodowych na strefy przetargowe
        climate_mapping = {
            'PL': {'climate': 'Continental', 'elevation': 173, 'coastal': 'Partial', 'urban': 'Mixed'},
            'DE': {'climate': 'Temperate Oceanic', 'elevation': 263, 'coastal': 'Partial', 'urban': 'High'},
            'FR': {'climate': 'Temperate', 'elevation': 375, 'coastal': 'Partial', 'urban': 'High'},
            'ES': {'climate': 'Mediterranean', 'elevation': 660, 'coastal': 'Extensive', 'urban': 'Medium'},
            'IT': {'climate': 'Mediterranean', 'elevation': 538, 'coastal': 'Extensive', 'urban': 'Medium'},
            'CZ': {'climate': 'Continental', 'elevation': 430, 'coastal': 'None', 'urban': 'Medium'},
            'SK': {'climate': 'Continental', 'elevation': 458, 'coastal': 'None', 'urban': 'Medium'},
            'AT': {'climate': 'Alpine', 'elevation': 910, 'coastal': 'None', 'urban': 'Medium'},
            'HU': {'climate': 'Continental', 'elevation': 143, 'coastal': 'None', 'urban': 'Medium'},
            'NL': {'climate': 'Temperate Oceanic', 'elevation': 30, 'coastal': 'Extensive', 'urban': 'High'},
            'BE': {'climate': 'Temperate Oceanic', 'elevation': 181, 'coastal': 'Partial', 'urban': 'High'},
            'LU': {'climate': 'Temperate Oceanic', 'elevation': 325, 'coastal': 'None', 'urban': 'High'},
            'IE': {'climate': 'Maritime', 'elevation': 118, 'coastal': 'Extensive', 'urban': 'Medium'},
            'DK': {'climate': 'Temperate Oceanic', 'elevation': 34, 'coastal': 'Extensive', 'urban': 'High'},
            'SE': {'climate': 'Subarctic', 'elevation': 320, 'coastal': 'Extensive', 'urban': 'Medium'},
            'FI': {'climate': 'Subarctic', 'elevation': 164, 'coastal': 'Extensive', 'urban': 'Medium'},
            'PT': {'climate': 'Mediterranean', 'elevation': 372, 'coastal': 'Extensive', 'urban': 'Medium'},
            'GR': {'climate': 'Mediterranean', 'elevation': 498, 'coastal': 'Extensive', 'urban': 'Medium'},
            'RO': {'climate': 'Continental', 'elevation': 414, 'coastal': 'Partial', 'urban': 'Medium'},
            'BG': {'climate': 'Continental', 'elevation': 472, 'coastal': 'Partial', 'urban': 'Medium'},
            'HR': {'climate': 'Mediterranean/Continental', 'elevation': 331, 'coastal': 'Extensive', 'urban': 'Medium'},
            'SI': {'climate': 'Alpine/Mediterranean', 'elevation': 492, 'coastal': 'Partial', 'urban': 'Medium'},
            'LT': {'climate': 'Continental', 'elevation': 110, 'coastal': 'Partial', 'urban': 'Medium'},
            'LV': {'climate': 'Continental', 'elevation': 87, 'coastal': 'Partial', 'urban': 'Medium'},
            'EE': {'climate': 'Continental', 'elevation': 61, 'coastal': 'Extensive', 'urban': 'Medium'}
        }
        
        # Pobierz ID stref przetargowych z ramki danych
        bidding_zone_id_map = {}
        bidding_zone_code_map = {}
        
        if not bidding_zones.empty and 'bidding_zone_code' in bidding_zones.columns:
            for idx, zone in bidding_zones.iterrows():
                bz_id = idx + 1  # ID bazuje na indeksie (numerowane od 1)
                bidding_zone_id_map[zone['bidding_zone_code']] = bz_id
                bidding_zone_code_map[bz_id] = zone['bidding_zone_code']
        
        for zone_code, zone_info in self.bidding_zone_mapping.items():
            country = zone_info['country']
            climate_info = climate_mapping.get(country, {
                'climate': 'Unknown', 'elevation': 0, 'coastal': 'Unknown', 'urban': 'Unknown'
            })
            
            bidding_zone_id = bidding_zone_id_map.get(zone_code, 0)
            
            weather_zone_record = {
                'weather_zone_name': f"{zone_info['name']} Weather Zone",
                'bidding_zone_id': bidding_zone_id,  # Używamy ID zamiast kodu
                'climate_zone': climate_info['climate'],
                'elevation_avg': climate_info['elevation'],
                'coastal_proximity': climate_info['coastal'],
                'urbanization_level': climate_info['urban'],
                # Dodatkowe pola dla SCD Type 2
                'valid_from': current_date,
                'valid_to': date(9999, 12, 31),
                'is_current': 'Yes'
            }
            
            # Generowanie business key i hash dla SCD Type 2
            weather_zone_record['business_key'] = f"{zone_code}_weather"
            weather_zone_record['row_hash'] = self._generate_row_hash(weather_zone_record, 
                ['weather_zone_name', 'bidding_zone_id', 'climate_zone', 'elevation_avg', 'coastal_proximity', 'urbanization_level'])
            
            weather_zones_list.append(weather_zone_record)
        
        return pd.DataFrame(weather_zones_list)
    
    def generate_socioeconomic_profile_dimension(self, socioeconomic_data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Generowanie wymiaru profili socjoekonomicznych z uwzględnieniem danych historycznych
        gdzie dane z roku X obowiązują od 1 stycznia roku X+1
        
        Args:
            socioeconomic_data: DataFrame z danymi socjoekonomicznymi
            
        Returns:
            DataFrame z wymiarem profili socjoekonomicznych
        """
        self.logger.info("Generating socioeconomic profile dimension")
        
        # Jeśli nie dostarczono danych socjoekonomicznych, wczytaj je ze staging
        if socioeconomic_data is None or socioeconomic_data.empty:
            socioeconomic_data = self.load_socioeconomic_data_from_staging()
        
        if socioeconomic_data is None or socioeconomic_data.empty:
            self.logger.warning("No socioeconomic data available. Using default values.")
            return pd.DataFrame()
        
        profiles_list = []
        
        # Mapowanie krajów na strefy przetargowe
        country_to_bidding_zone = {}
        for zone_code, zone_info in self.bidding_zone_mapping.items():
            country_to_bidding_zone[zone_info['country']] = zone_code
        
        # Grupuj dane wg kraju
        for country_code, country_group in socioeconomic_data.groupby('country_code'):
            # Posortuj dane wg roku (od najstarszych)
            country_data = country_group.sort_values('year')
            
            # Mapowanie strefy przetargowej na podstawie kodu kraju
            bidding_zone_code = country_to_bidding_zone.get(country_code)
            
            if bidding_zone_code is None:
                self.logger.warning(f"No bidding zone found for country code {country_code}. Skipping.")
                continue
            
            # Przetwarzamy każdy rok
            for i, (_, data) in enumerate(country_data.iterrows()):
                year = int(data['year'])
                
                # Ustal okres obowiązywania:
                # - Dane z roku X obowiązują od 1 stycznia roku X+1
                # - Jeśli to ostatni rekord dla kraju, obowiązuje do 31.12.9999
                # - W przeciwnym razie obowiązuje do końca roku (31 grudnia)
                valid_from = date(year + 1, 1, 1)
                
                if i < len(country_data) - 1:
                    next_year = int(country_data.iloc[i + 1]['year'])
                    valid_to = date(next_year + 1, 1, 1) - timedelta(days=1)
                    is_current = 'No'
                else:
                    valid_to = date(9999, 12, 31)
                    is_current = 'Yes'
                
                # Wykorzystujemy dane bezpośrednio z tabeli staging
                profile_record = {
                    'bidding_zone_code': bidding_zone_code,
                    'country_code': country_code,
                    'country_name': data['country_name'],
                    'year': year,
                    'avg_income_level': data['gdp_per_capita'] if pd.notna(data['gdp_per_capita']) else 0.0,
                    'unemployment_rate': data['unemployment_rate'] if pd.notna(data['unemployment_rate']) else 0.0,
                    'urbanization_rate': data['poverty_by_degree_of_urbanization'] if pd.notna(data['poverty_by_degree_of_urbanization']) else 0.0,
                    'service_sector_percentage': data['service_sector_percentage'] if pd.notna(data['service_sector_percentage']) else 0.0,
                    'industry_sector_percentage': data['industry_sector_percentage'] if pd.notna(data['industry_sector_percentage']) else 0.0,
                    'energy_poverty_rate': data['energy_poverty_rate'] if pd.notna(data['energy_poverty_rate']) else 0.0,
                    'residential_percentage': 30.0,  # Szacunkowy udział sektora mieszkaniowego - brak w danych
                    'commercial_percentage': 25.0,   # Szacunkowy udział sektora komercyjnego - brak w danych
                    'industrial_percentage': 45.0,   # Szacunkowy udział sektora przemysłowego - brak w danych
                    'avg_household_size': data['avg_household_size'] if pd.notna(data['avg_household_size']) else 0.0,
                    'primary_heating_type': data['primary_heating_type'] if pd.notna(data['primary_heating_type']) and data['primary_heating_type'] else 'Natural Gas',
                    'population': data['population'] if pd.notna(data['population']) else 0.0,
                    # Pola dla SCD Type 2
                    'valid_from': valid_from,
                    'valid_to': valid_to,
                    'is_current': is_current
                }
                
                # Generowanie business key i hash dla SCD Type 2
                profile_record['business_key'] = f"{country_code}_{year}"
                profile_record['row_hash'] = self._generate_row_hash(profile_record, 
                    ['country_code', 'year', 'avg_income_level', 'unemployment_rate', 
                     'urbanization_rate', 'service_sector_percentage', 'industry_sector_percentage', 
                     'energy_poverty_rate', 'avg_household_size', 'primary_heating_type', 'population'])
                
                profiles_list.append(profile_record)
        
        # Jeśli nie znaleziono żadnych profili, zwróć pusty DataFrame
        if not profiles_list:
            self.logger.warning("No socioeconomic profiles could be created from available data.")
            return pd.DataFrame()
        
        return pd.DataFrame(profiles_list)
    
    def _generate_default_socioeconomic_profiles(self) -> pd.DataFrame:
        """Generowanie domyślnych profili socjoekonomicznych z obsługą SCD Type 2"""
        default_profiles = []
        current_date = datetime.now().date()
        current_year = datetime.now().year
        
        # Pobierz ID stref przetargowych z bazy danych
        bidding_zone_ids = self._get_bidding_zone_ids()
        
        for zone_code, zone_info in self.bidding_zone_mapping.items():
            bidding_zone_id = bidding_zone_ids.get(zone_code, 0)
            if bidding_zone_id == 0:
                self.logger.warning(f"No bidding zone ID found for code {zone_code}")
                continue
                
            profile_record = {
                'profile_name': 'Standard',
                'bidding_zone_id': bidding_zone_id,
                'year': current_year,
                'avg_income_level': 0.0,
                'unemployment_rate': 0.0,
                'urbanization_rate': self._estimate_urbanization_rate(zone_info['country']),
                'service_sector_percentage': self._estimate_service_sector(zone_info['country']),
                'industry_sector_percentage': self._estimate_industry_sector(zone_info['country']),
                'energy_poverty_rate': 0.0,
                'residential_percentage': 30.0,
                'commercial_percentage': 25.0,
                'industrial_percentage': 45.0,
                'avg_household_size': self._estimate_household_size(zone_info['country']),
                'primary_heating_type': self._determine_primary_heating(zone_info['country']),
                # Dodatkowe pola dla SCD Type 2
                'valid_from': current_date,
                'valid_to': date(9999, 12, 31),
                'is_current': 'Yes'
            }
            
            # Generowanie business key i hash dla SCD Type 2
            profile_record['business_key'] = f"{zone_info['country']}_{current_year}"
            profile_record['row_hash'] = self._generate_row_hash(profile_record, 
                ['profile_name', 'bidding_zone_id', 'year', 'avg_income_level', 'unemployment_rate', 
                 'urbanization_rate', 'service_sector_percentage', 'industry_sector_percentage', 
                 'energy_poverty_rate', 'residential_percentage', 'commercial_percentage', 
                 'industrial_percentage', 'avg_household_size', 'primary_heating_type'])
            
            default_profiles.append(profile_record)
        
        return pd.DataFrame(default_profiles)
        
    def process_all_dimensions(self, start_date: date = None, end_date: date = None,
                              socioeconomic_data: pd.DataFrame = None) -> Dict[str, pd.DataFrame]:
        """
        Przetwarzanie wszystkich wymiarów
        
        Args:
            start_date: Data początkowa dla wymiaru daty
            end_date: Data końcowa dla wymiaru daty
            socioeconomic_data: Dane socjoekonomiczne
            
        Returns:
            Słownik z DataFrame'ami wszystkich wymiarów
        """
        self.logger.info("Processing all dimensions")
        
        # Logowanie rozpoczęcia procesu
        self._log_process('DIMENSION_PROCESSING', 'RUNNING')
        
        try:
            dimensions = {}
            
            # Wymiar daty
            if start_date and end_date:
                dimensions['dim_date'] = self.generate_date_dimension(start_date, end_date)
            else:
                # Domyślny zakres: rok wstecz do roku w przód
                today = date.today()
                start_default = date(today.year - 1, 1, 1)
                end_default = date(today.year + 1, 12, 31)
                dimensions['dim_date'] = self.generate_date_dimension(start_default, end_default)
            
            # Wymiar czasu
            dimensions['dim_time'] = self.generate_time_dimension()
            
            # Wymiar stref przetargowych
            dimensions['dim_bidding_zone'] = self.process_bidding_zone_dimension(socioeconomic_data)
            
            # Wymiar typów generacji
            dimensions['dim_generation_type'] = self.generate_generation_type_dimension()
            
            # Wymiar warunków pogodowych
            dimensions['dim_weather_condition'] = self.generate_weather_condition_dimension()
            
            # Wymiar stref pogodowych
            dimensions['dim_weather_zone'] = self.generate_weather_zone_dimension(
                dimensions['dim_bidding_zone']
            )
            
            # Wymiar profili socjoekonomicznych
            dimensions['dim_socioeconomic_profile'] = self.generate_socioeconomic_profile_dimension(
                socioeconomic_data
            )
            
            # Podsumowanie
            total_records = sum(len(df) for df in dimensions.values())
            self.logger.info(f"Generated {len(dimensions)} dimensions with {total_records} total records")
            
            # Logowanie sukcesu
            self._log_process('DIMENSION_PROCESSING', 'SUCCESS', total_records)
            
            return dimensions
            
        except Exception as e:
            self.logger.error(f"Error processing dimensions: {str(e)}")
            self._log_process('DIMENSION_PROCESSING', 'FAILED', 0, str(e))
            raise
    def _generate_row_hash(self, record: Dict, columns_to_hash: List[str]) -> str:
        """
        Generuje hash wiersza dla SCD Type 2
        
        Args:
            record: Rekord do wygenerowania hasha
            columns_to_hash: Lista kolumn, które mają być uwzględnione w hashu
            
        Returns:
            MD5 hash jako string
        """
        import hashlib
        import json
        
        # Tworzenie podsłownika tylko z kolumnami, które mają być uwzględnione w hashu
        hash_dict = {}
        for col in columns_to_hash:
            if col in record:
                hash_dict[col] = record[col]
        
        # Konwersja słownika na string i generowanie MD5 hasha
        hash_string = json.dumps(hash_dict, sort_keys=True, default=str)
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def _get_bidding_zone_ids(self) -> Dict[str, int]:
        """
        Pobiera mapowanie kodów stref przetargowych na ID
        
        Returns:
            Słownik mapujący kody stref na ID
        """
        # Generujemy ID na podstawie indeksów w mapowaniu
        bidding_zone_ids = {}
        for i, zone_code in enumerate(self.bidding_zone_mapping.keys(), 1):
            bidding_zone_ids[zone_code] = i
        
        return bidding_zone_ids
        
        
        
        # Funkcja główna do uruchomienia z SSIS
def main():
    """Główna funkcja wywoływana przez SSIS"""
    import sys
    import os
    from datetime import date, datetime
    
    # Konfiguracja logowania na poziomie głównej funkcji
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("DimensionProcessorMain")
    
    logger.info("Starting Dimension Processor")
    
    # Parametry z SSIS lub zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Jeśli nie podano connection_string, użyj domyślnego
    if not connection_string:
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        logger.info(f"Using default connection string: {connection_string}")
    
    # Domyślny zakres dat
    today = date.today()
    start_date = date(today.year - 2, 12, 29)
    end_date = date(today.year + 1, 12, 31)
    
    # Parametry z argumentów linii komend (opcjonalnie)
    if len(sys.argv) > 1:
        try:
            start_date_str = sys.argv[1]
            if start_date_str.startswith('"') and start_date_str.endswith('"'):
                start_date_str = start_date_str[1:-1]
            start_date = datetime.fromisoformat(start_date_str).date()
            logger.info(f"Using start_date from arguments: {start_date}")
        except Exception as e:
            logger.error(f"Error parsing start_date: {e}, using default: {start_date}")
    
    if len(sys.argv) > 2:
        try:
            end_date_str = sys.argv[2]
            if end_date_str.startswith('"') and end_date_str.endswith('"'):
                end_date_str = end_date_str[1:-1]
            end_date = datetime.fromisoformat(end_date_str).date()
            logger.info(f"Using end_date from arguments: {end_date}")
        except Exception as e:
            logger.error(f"Error parsing end_date: {e}, using default: {end_date}")
    
    # Parametr określający czy wykonać aktualizację SCD Type 2
    update_scd2 = False
    if len(sys.argv) > 3:
        update_scd2 = sys.argv[3].lower() in ('true', 'yes', '1', 't', 'y')
    
    logger.info(f"Dimension processing period: {start_date} to {end_date}")
    logger.info(f"SCD2 update enabled: {update_scd2}")
    
    try:
        # Inicjalizacja procesora wymiarów
        processor = DimensionProcessor(connection_string)
        
        # Pobranie danych socjoekonomicznych ze staging (jeśli dostępne)
        socioeconomic_data = None
        try:
            socioeconomic_data = processor.load_socioeconomic_data_from_staging()
        except Exception as e:
            logger.warning(f"Could not load socioeconomic data from staging: {e}")
            logger.info("Will use default socioeconomic profiles")
        
        # Przetwarzanie wszystkich wymiarów
        logger.info(f"Processing dimensions for period: {start_date} to {end_date}")
        dimensions = processor.process_all_dimensions(
            start_date, end_date, socioeconomic_data
        )
        
        # Zapis do staging
        logger.info("Saving dimensions to staging tables")
        staging_success = processor.save_dimensions_to_staging(dimensions)
        
        # Aktualizacja SCD Type 2 (opcjonalnie)
        scd2_success = True
        if update_scd2:
            logger.info("Updating dimensions with SCD Type 2")
            scd2_success = processor.update_dimensions_with_scd2()
        
        if staging_success and scd2_success:
            logger.info("Dimension processing completed successfully")
            sys.exit(0)
        else:
            logger.error("Error occurred during dimension processing")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()