"""
FixNullValues.py
Usuwanie wartości NULL z wszystkich tabel hurtowni danych
Zastępowanie braków danych wartościami domyślnymi zgodnie z dokumentacją
"""

import pyodbc
import logging
import os
import sys
from datetime import datetime

class NullValuesFixer:
    """Klasa do usuwania wartości NULL z hurtowni danych"""
    
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
        
        # Mapowanie wartości domyślnych dla różnych typów kolumn
        self.default_values = {
            # Wartości tekstowe
            'text_defaults': {
                'country_code': 'UNK',
                'country_name': 'Unknown',
                'bidding_zone_code': 'UNKNOWN',
                'bidding_zone_name': 'Unknown Zone',
                'weather_zone_name': 'Unknown Zone',
                'zone_code': 'UNKNOWN',
                'zone_name': 'Unknown Zone',
                'subzone_code': 'UNK',
                'subzone_name': 'Unknown Subzone',
                'entso_code': 'B20',
                'generation_category': 'Unknown',
                'generation_type': 'Unknown',
                'fuel_source': 'Unknown',
                'condition_type': 'Normal',
                'condition_severity': 'None',
                'extreme_weather_type': 'None',
                'weather_condition': 'Normal',
                'primary_country': 'Unknown',
                'secondary_countries': '',
                'control_area': 'Unknown',
                'timezone': 'UTC',
                'climate_zone': 'Unknown',
                'coastal_proximity': 'Unknown',
                'urbanization_level': 'Unknown',
                'primary_heating_type': 'Unknown',
                'day_of_week': 'Unknown',
                'month_name': 'Unknown',
                'season': 'Unknown',
                'holiday_name': '',
                'holiday_type': '',
                'day_period': 'Unknown',
                'data_type': 'Unknown',
                'bidding_zone': 'Unknown'
            },
            
            # Wartości binarne (tak/nie)
            'binary_defaults': {
                'is_intermittent': 'No',
                'is_extreme_weather': 'No',
                'is_holiday': 'No',
                'is_school_day': 'No',
                'is_weekend': 'No',
                'is_peak_hour': 'No'
            },
            
            # Wartości liczbowe (0 dla identyfikatorów)
            'numeric_id_defaults': {
                'day_of_month': 0,
                'month': 0,
                'quarter': 0,
                'year': 0,
                'hour': 0,
                'minute': 0,
                'wind_direction': -999,  # Kierunek wiatru -999 (poza zakresem 0-360)
                'population': 0,
                'data_quality_score': 0
            },
            
            # Wartości liczbowe (0.00 dla miar gdzie zero ma sens)
            'numeric_measure_defaults': {
                'elevation_avg': 0.00,  # Wysokość może być 0 (poziom morza)
                'avg_household_size': 0.00,  # Może być 0 w niektórych przypadkach
                'poverty_by_degree_of_urbanization': -99.99,  # Procent nie może być ujemny
                'actual_consumption': 0.00,  # 0 MWh ma sens
                'forecasted_consumption': 0.00,  # 0 MWh ma sens  
                'consumption_deviation': 0.00,  # Może być rzeczywiście 0
                'generation_amount': 0.00,  # 0 MWh ma sens (brak generacji)
                'heating_degree_days': 0.00,  # Może być 0 w ciepłym klimacie
                'cooling_degree_days': 0.00,  # Może być 0 w chłodnym klimacie
                'quantity': 0.00,  # Może być 0
                'temperature_mean': -99.99  # Specjalna wartość dla temperatury
            },
            
            # Wartości niemożliwe fizycznie/logicznie (-99.99)
            'impossible_values_defaults': {
                'gdp_per_capita': -99.99,  # PIB nie może być ujemne
                'energy_intensity': -99.99,  # Intensywność energetyczna nie może być ujemna
                'electricity_price_avg': -99.99,  # Cena nie może być ujemna
                'avg_income_level': -99.99,  # Dochód nie może być ujemny
                'unemployment_rate': -99.99,  # Procent nie może być ujemny
                'urbanization_rate': -99.99,  # Procent nie może być ujemny
                'service_sector_percentage': -99.99,  # Procent nie może być ujemny
                'industry_sector_percentage': -99.99,  # Procent nie może być ujemny
                'energy_poverty_rate': -99.99,  # Procent nie może być ujemny
                'residential_percentage': -99.99,  # Procent nie może być ujemny
                'commercial_percentage': -99.99,  # Procent nie może być ujemny
                'industrial_percentage': -99.99,  # Procent nie może być ujemny
                'capacity_factor': -99.99,  # Procent nie może być ujemny (0-100%)
                'renewable_percentage': -99.99,  # Procent nie może być ujemny
                'per_capita_consumption': -99.99,  # Zużycie na osobę nie może być ujemne
                'humidity': -99.99,  # Wilgotność nie może być ujemna (0-100%)
                'precipitation': -99.99,  # Opady nie mogą być ujemne
                'wind_speed': -99.99,  # Prędkość wiatru nie może być ujemna
                'cloud_cover': -99.99,  # Zachmurzenie nie może być ujemne (0-100%)
                'solar_radiation': -99.99,  # Promieniowanie nie może być ujemne
                'air_pressure': -99.99  # Ciśnienie nie może być ujemne
            },
            
            # Współrzędne geograficzne (poza zakresem)
            'geographic_defaults': {
                'latitude': -999.99,  # Poza zakresem -90 do +90
                'longitude': -999.99  # Poza zakresem -180 do +180
            },
            
            # Wartości specjalne dla temperatur
            'temperature_defaults': {
                'temperature_avg': -99.99,
                'temperature_min': -99.99,
                'temperature_max': -99.99
            },
            
            # Daty specjalne
            'date_defaults': {
                'full_date': '1900-01-01',
                'date': '1900-01-01',
                'timestamp': '1900-01-01 00:00:00',
                'created_at': '1900-01-01 00:00:00'
            }
        }
    
    def get_all_tables(self) -> list:
        """
        Pobieranie listy tabel wymiarowych (dim_*) i faktów (fact_*)
        
        Returns:
            Lista nazw tabel
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND (TABLE_NAME LIKE 'dim_%' OR TABLE_NAME LIKE 'fact_%')
                ORDER BY 
                    CASE WHEN TABLE_NAME LIKE 'dim_%' THEN 1 ELSE 2 END,
                    TABLE_NAME
            """)
            
            tables = [row.TABLE_NAME for row in cursor.fetchall()]
            conn.close()
            
            self.logger.info(f"Found {len(tables)} dimension and fact tables to process")
            return tables
            
        except Exception as e:
            self.logger.error(f"Error getting table list: {str(e)}")
            return []
    
    def get_table_columns_with_nulls(self, table_name: str) -> list:
        """
        Pobieranie WSZYSTKICH kolumn tabeli (poza IDENTITY)
        
        Args:
            table_name: Nazwa tabeli
            
        Returns:
            Lista słowników z informacjami o kolumnach
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Pobierz informacje o kolumnach (poza IDENTITY)
            cursor.execute(f"""
                SELECT 
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.NUMERIC_PRECISION,
                    c.NUMERIC_SCALE,
                    COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = '{table_name}'
                AND COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 0
                ORDER BY c.ORDINAL_POSITION
            """)
            
            columns_info = []
            for row in cursor.fetchall():
                col_info = {
                    'name': row.COLUMN_NAME,
                    'data_type': row.DATA_TYPE,
                    'is_nullable': row.IS_NULLABLE == 'YES',
                    'max_length': row.CHARACTER_MAXIMUM_LENGTH,
                    'precision': row.NUMERIC_PRECISION,
                    'scale': row.NUMERIC_SCALE,
                    'is_identity': row.IS_IDENTITY == 1
                }
                
                # Sprawdź czy kolumna zawiera NULL-e
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {row.COLUMN_NAME} IS NULL")
                null_count = cursor.fetchone()[0]
                col_info['null_count'] = null_count
                
                columns_info.append(col_info)
            
            conn.close()
            return columns_info
            
        except Exception as e:
            self.logger.error(f"Error getting columns for table {table_name}: {str(e)}")
            return []
    
    def get_default_value_for_column(self, column_name: str, data_type: str) -> str:
        """
        Określenie wartości domyślnej dla kolumny na podstawie nazwy i typu
        
        Args:
            column_name: Nazwa kolumny
            data_type: Typ danych kolumny
            
        Returns:
            Wartość domyślna jako string gotowy do SQL
        """
        # Sprawdź czy kolumna ma specjalną wartość domyślną w poszczególnych kategoriach
        all_defaults = {
            **self.default_values['text_defaults'],
            **self.default_values['binary_defaults'], 
            **self.default_values['numeric_id_defaults'],
            **self.default_values['numeric_measure_defaults'],
            **self.default_values['impossible_values_defaults'],
            **self.default_values['geographic_defaults'],
            **self.default_values['temperature_defaults'],
            **self.default_values['date_defaults']
        }
        
        if column_name in all_defaults:
            value = all_defaults[column_name]
            if isinstance(value, str):
                return f"'{value}'"
            else:
                return str(value)
        
        # Wartości domyślne na podstawie typu danych i nazwy kolumny
        if data_type.lower() in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
            # Dla kolumn tekstowych
            if 'code' in column_name.lower():
                return "'UNK'"
            elif 'name' in column_name.lower():
                return "'Unknown'"
            elif 'type' in column_name.lower():
                return "'Unknown'"
            elif 'zone' in column_name.lower():
                return "'Unknown'"
            else:
                return "'Unknown'"
                
        elif data_type.lower() in ['int', 'bigint', 'smallint', 'tinyint']:
            return "0"
            
        elif data_type.lower() in ['decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']:
            # Dla parametrów meteorologicznych, ekonomicznych i procentowych - użyj -99.99
            meteorological_params = ['humidity', 'precipitation', 'wind_speed', 'cloud_cover', 
                                   'solar_radiation', 'air_pressure']
            economic_params = ['gdp_per_capita', 'energy_intensity', 'electricity_price_avg',
                             'avg_income_level', 'unemployment_rate', 'urbanization_rate']
            percentage_params = ['service_sector_percentage', 'industry_sector_percentage',
                               'energy_poverty_rate', 'residential_percentage', 'commercial_percentage',
                               'industrial_percentage', 'capacity_factor', 'renewable_percentage']
            energy_params = ['per_capita_consumption']
            coordinate_params = ['latitude', 'longitude']
            
            if any(param in column_name.lower() for param in meteorological_params):
                return "-99.99"
            elif any(param in column_name.lower() for param in economic_params):
                return "-99.99"  
            elif any(param in column_name.lower() for param in percentage_params):
                return "-99.99"
            elif any(param in column_name.lower() for param in energy_params):
                return "-99.99"
            elif any(param in column_name.lower() for param in coordinate_params):
                return "-999.99"
            elif 'temperature' in column_name.lower():
                return "-99.99"
            elif 'direction' in column_name.lower():
                return "-999"  # Kierunek wiatru
            else:
                return "0.00"  # Dla pozostałych miar gdzie 0 ma sens
                
        elif data_type.lower() in ['date']:
            return "'1900-01-01'"
            
        elif data_type.lower() in ['datetime', 'datetime2', 'smalldatetime']:
            return "'1900-01-01 00:00:00'"
            
        elif data_type.lower() in ['bit']:
            return "0"
            
        else:
            # Domyślnie dla nieznanych typów
            return "'Unknown'"
    
    def fix_null_values_in_table(self, table_name: str) -> bool:
        """
        Usuwanie wartości NULL z konkretnej tabeli i ustawianie wszystkich kolumn jako NOT NULL
        
        Args:
            table_name: Nazwa tabeli
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info(f"Processing table: {table_name}")
        
        try:
            all_columns = self.get_table_columns_with_nulls(table_name)
            
            if not all_columns:
                self.logger.warning(f"No columns found in table {table_name}")
                return True
            
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            total_updates = 0
            columns_with_nulls = []
            
            # Najpierw napraw wszystkie NULL-e (włączając created_at)
            for col_info in all_columns:
                column_name = col_info['name']
                data_type = col_info['data_type']
                null_count = col_info['null_count']
                
                if null_count > 0:
                    columns_with_nulls.append(col_info)
                    
                    # Specjalna logika dla created_at
                    if column_name == 'created_at':
                        default_value = "'1900-01-01 00:00:00'"
                    else:
                        default_value = self.get_default_value_for_column(column_name, data_type)
                    
                    self.logger.info(f"  Updating {null_count} NULL values in column {column_name} with {default_value}")
                    
                    # Wykonaj UPDATE
                    update_sql = f"""
                        UPDATE {table_name} 
                        SET {column_name} = {default_value}
                        WHERE {column_name} IS NULL
                    """
                    
                    cursor.execute(update_sql)
                    updated_rows = cursor.rowcount
                    total_updates += updated_rows
                    
                    self.logger.info(f"  Updated {updated_rows} rows in column {column_name}")
            
            conn.commit()
            
            # Teraz ustaw WSZYSTKIE kolumny jako NOT NULL (oprócz IDENTITY)
            self.logger.info(f"Setting ALL columns to NOT NULL in table {table_name}")
            
            for col_info in all_columns:
                if col_info['is_nullable']:
                    column_name = col_info['name']
                    data_type = col_info['data_type']
                    max_length = col_info['max_length']
                    precision = col_info['precision']
                    scale = col_info['scale']
                    
                    # Buduj definicję typu
                    if data_type.upper() in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR']:
                        if max_length and max_length > 0:
                            type_definition = f"{data_type}({max_length})"
                        else:
                            type_definition = f"{data_type}(MAX)"
                    elif data_type.upper() in ['DECIMAL', 'NUMERIC']:
                        if precision and scale is not None:
                            type_definition = f"{data_type}({precision},{scale})"
                        else:
                            type_definition = data_type
                    elif data_type.upper() in ['DATETIME2']:
                        # Dla DATETIME2 sprawdź czy ma precyzję
                        if scale is not None and scale > 0:
                            type_definition = f"{data_type}({scale})"
                        else:
                            type_definition = data_type
                    else:
                        type_definition = data_type
                    
                    # Wykonaj ALTER TABLE
                    alter_sql = f"""
                        ALTER TABLE {table_name} 
                        ALTER COLUMN {column_name} {type_definition} NOT NULL
                    """
                    
                    try:
                        self.logger.info(f"  Attempting to set {column_name} ({type_definition}) to NOT NULL")
                        cursor.execute(alter_sql)
                        conn.commit()
                        self.logger.info(f"  ✓ Successfully set {column_name} to NOT NULL")
                    except Exception as e:
                        self.logger.error(f"  ✗ Failed to set {column_name} to NOT NULL: {str(e)}")
                        # Pokaż dokładny SQL który się nie wykonał
                        self.logger.error(f"  Failed SQL: {alter_sql}")
                        conn.rollback()
                        return False
                else:
                    self.logger.info(f"  Column {col_info['name']} already NOT NULL")
            
            conn.close()
            
            self.logger.info(f"Completed table {table_name}. Total NULL updates: {total_updates}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing table {table_name}: {str(e)}")
            return False
    
    def verify_no_nulls_and_all_not_null(self) -> bool:
        """
        Weryfikacja że nie ma wartości NULL i wszystkie kolumny są NOT NULL
        
        Returns:
            True jeśli wszystko w porządku, False w przeciwnym razie
        """
        self.logger.info("Verifying that no NULL values remain and all columns are NOT NULL")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            tables = self.get_all_tables()
            total_nulls = 0
            nullable_columns = 0
            
            for table_name in tables:
                self.logger.info(f"Verifying table: {table_name}")
                
                # Pobierz wszystkie kolumny tabeli (oprócz IDENTITY)
                cursor.execute(f"""
                    SELECT 
                        c.COLUMN_NAME,
                        c.IS_NULLABLE,
                        COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_NAME = '{table_name}'
                    AND COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 0
                """)
                
                columns = cursor.fetchall()
                
                for row in columns:
                    column_name = row.COLUMN_NAME
                    is_nullable = row.IS_NULLABLE == 'YES'
                    
                    # Sprawdź wartości NULL
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")
                    null_count = cursor.fetchone()[0]
                    
                    if null_count > 0:
                        self.logger.error(f"Found {null_count} NULL values in {table_name}.{column_name}")
                        total_nulls += null_count
                    
                    # Sprawdź czy kolumna jest nullable
                    if is_nullable:
                        self.logger.error(f"Column {table_name}.{column_name} is still NULLABLE")
                        nullable_columns += 1
                    else:
                        self.logger.debug(f"Column {table_name}.{column_name} is correctly NOT NULL")
            
            conn.close()
            
            success = True
            if total_nulls == 0:
                self.logger.info("✓ No NULL values found")
            else:
                self.logger.error(f"✗ Found {total_nulls} NULL values")
                success = False
                
            if nullable_columns == 0:
                self.logger.info("✓ All columns are NOT NULL")
            else:
                self.logger.error(f"✗ Found {nullable_columns} columns that are still NULLABLE")
                success = False
            
            if success:
                self.logger.info("Verification successful: All columns are NOT NULL and contain no NULL values")
            else:
                self.logger.error("Verification failed")
                
            return success
                
        except Exception as e:
            self.logger.error(f"Error during verification: {str(e)}")
            return False
    
    def run_full_null_fix(self) -> bool:
        """
        Przeprowadzenie pełnego usunięcia wartości NULL i ustawienia wszystkich kolumn jako NOT NULL
        
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Starting full NULL values fix for dimension and fact tables")
        
        # 1. Pobierz listę tabel wymiarowych i faktów
        tables = self.get_all_tables()
        if not tables:
            self.logger.error("No dimension or fact tables found")
            return False
        
        self.logger.info(f"Processing {len(tables)} tables: {', '.join(tables)}")
        
        # 2. Przetwórz każdą tabelę (usuń NULL-e i ustaw NOT NULL)
        success_count = 0
        for table_name in tables:
            if self.fix_null_values_in_table(table_name):
                success_count += 1
            else:
                self.logger.error(f"Failed to process table {table_name}")
                return False  # Zatrzymaj przy pierwszym błędzie
        
        self.logger.info(f"Successfully processed {success_count}/{len(tables)} tables")
        
        # 3. Weryfikacja końcowa
        if self.verify_no_nulls_and_all_not_null():
            self.logger.info("NULL values fix completed successfully - all columns are NOT NULL")
            return True
        else:
            self.logger.error("NULL values fix completed with errors")
            return False

def main():
    """Główna funkcja wywoływana z konsoli"""
    import sys
    import os
    
    # Konfiguracja logowania
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                       handlers=[
                           logging.FileHandler("fix_null_values.log"),
                           logging.StreamHandler()
                       ])
    logger = logging.getLogger("NullValuesFixerMain")
    
    logger.info("Starting NULL Values Fix for Dimension and Fact Tables")
    
    # Parametry z zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Jeśli nie podano connection_string, użyj domyślnego
    if not connection_string:
        connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=EnergyWeatherDW1;Trusted_Connection=yes;"
        logger.info(f"Using default connection string: {connection_string}")
    
    try:
        # Inicjalizacja NullValuesFixer
        fixer = NullValuesFixer(connection_string)
        
        # Uruchomienie pełnego usunięcia NULL-i
        success = fixer.run_full_null_fix()
        
        if success:
            logger.info("✓ NULL values fix completed successfully - all dimension and fact tables now have NOT NULL columns")
            sys.exit(0)
        else:
            logger.error("✗ NULL values fix failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()