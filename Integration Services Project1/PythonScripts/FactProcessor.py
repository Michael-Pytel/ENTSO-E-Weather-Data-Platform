"""
FactProcessor.py
Przetwarzanie danych dla tabeli faktów
Łączenie danych energetycznych z pogodowymi, obliczanie metryk pochodnych
"""

import pandas as pd
from datetime import datetime, timedelta
import pyodbc
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
import traceback

class FactProcessor:
    """Procesor do tworzenia i przetwarzania tabeli faktów"""
    
    def __init__(self, connection_string: str):
        """
        Inicjalizacja procesora faktów
        
        Args:
            connection_string: String połączenia z bazą danych
        """
        self.connection_string = connection_string
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Cache dla mapowań wymiarów
        self.dimension_cache = {}
    
    def load_dimension_mappings(self) -> Dict[str, pd.DataFrame]:
        """
        Ładowanie mapowań wymiarów z tabel STAGING (nie z docelowych tabel wymiarowych)
        
        Returns:
            Słownik z DataFrame'ami wymiarów
        """
        self.logger.info("Loading dimension mappings from staging tables")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            
            # Ładowanie wymiarów ze STAGING
            dimensions = {}
            
            # Wymiar daty
            try:
                dimensions['date'] = pd.read_sql("""
                    SELECT id as staging_date_id, full_date, year, month, day_of_month
                    FROM staging_dim_date
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['date'])} date dimension records")
            except Exception as e:
                self.logger.error(f"Error loading date dimension: {str(e)}")
                dimensions['date'] = pd.DataFrame()
            
            # Wymiar czasu
            try:
                dimensions['time'] = pd.read_sql("""
                    SELECT id as staging_time_id, hour, minute
                    FROM staging_dim_time
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['time'])} time dimension records")
            except Exception as e:
                self.logger.error(f"Error loading time dimension: {str(e)}")
                dimensions['time'] = pd.DataFrame()
            
            # Wymiar stref przetargowych
            try:
                dimensions['bidding_zone'] = pd.read_sql("""
                    SELECT id as staging_bidding_zone_id, bidding_zone_code, year
                    FROM staging_dim_bidding_zone
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['bidding_zone'])} bidding zone dimension records")
            except Exception as e:
                self.logger.error(f"Error loading bidding zone dimension: {str(e)}")
                dimensions['bidding_zone'] = pd.DataFrame()
            
            # Wymiar stref pogodowych
            try:
                dimensions['weather_zone'] = pd.read_sql("""
                    SELECT id as staging_weather_zone_id, weather_zone_name, bidding_zone_id
                    FROM staging_dim_weather_zone
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['weather_zone'])} weather zone dimension records")
            except Exception as e:
                self.logger.error(f"Error loading weather zone dimension: {str(e)}")
                dimensions['weather_zone'] = pd.DataFrame()
            
            # Wymiar typów generacji
            try:
                dimensions['generation_type'] = pd.read_sql("""
                    SELECT id as staging_generation_type_id, entso_code, generation_type, generation_category
                    FROM staging_dim_generation_type
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['generation_type'])} generation type dimension records")
            except Exception as e:
                self.logger.error(f"Error loading generation type dimension: {str(e)}")
                dimensions['generation_type'] = pd.DataFrame()
            
            # Wymiar warunków pogodowych
            try:
                dimensions['weather_condition'] = pd.read_sql("""
                    SELECT id as staging_weather_condition_id, condition_type, condition_severity
                    FROM staging_dim_weather_condition
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['weather_condition'])} weather condition dimension records")
            except Exception as e:
                self.logger.error(f"Error loading weather condition dimension: {str(e)}")
                dimensions['weather_condition'] = pd.DataFrame()
            
            # Wymiar profilu socjoekonomicznego
            try:
                dimensions['socioeconomic_profile'] = pd.read_sql("""
                    SELECT id as staging_socioeconomic_profile_id, bidding_zone_code, year
                    FROM staging_dim_socioeconomic_profile
                """, conn)
                self.logger.info(f"Loaded {len(dimensions['socioeconomic_profile'])} socioeconomic profile dimension records")
            except Exception as e:
                self.logger.error(f"Error loading socioeconomic profile dimension: {str(e)}")
                dimensions['socioeconomic_profile'] = pd.DataFrame()
            
            conn.close()
            
            # Cache wymiarów
            self.dimension_cache = dimensions
            
            return dimensions
            
        except Exception as e:
            self.logger.error(f"Error loading dimension mappings: {str(e)}")
            self.logger.error(traceback.format_exc())
            return {}
    
    def load_staging_data(self) -> Dict[str, pd.DataFrame]:
        """
        Ładowanie danych ze staging tables
        
        Returns:
            Słownik z danymi staging
        """
        self.logger.info("Loading data from staging tables")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            
            staging_data = {}
            
            # Dane energetyczne ENTSO-E
            try:
                staging_data['entso_actual_load'] = pd.read_sql("""
                    SELECT * FROM staging_entso_actual_load
                """, conn)
                self.logger.info(f"Loaded {len(staging_data['entso_actual_load'])} actual load records")
            except Exception as e:
                self.logger.error(f"Error loading actual load data: {str(e)}")
                staging_data['entso_actual_load'] = pd.DataFrame()
            
            try:
                staging_data['entso_generation'] = pd.read_sql("""
                    SELECT * FROM staging_entso_generation
                """, conn)
                self.logger.info(f"Loaded {len(staging_data['entso_generation'])} generation records")
            except Exception as e:
                self.logger.error(f"Error loading generation data: {str(e)}")
                staging_data['entso_generation'] = pd.DataFrame()
            
            try:
                staging_data['entso_forecast'] = pd.read_sql("""
                    SELECT * FROM staging_entso_forecast
                """, conn)
                self.logger.info(f"Loaded {len(staging_data['entso_forecast'])} forecast records")
            except Exception as e:
                self.logger.error(f"Error loading forecast data: {str(e)}")
                staging_data['entso_forecast'] = pd.DataFrame()
            
            # Dane pogodowe
            try:
                staging_data['weather'] = pd.read_sql("""
                    SELECT * FROM staging_weather_data
                """, conn)
                self.logger.info(f"Loaded {len(staging_data['weather'])} weather records")
            except Exception as e:
                self.logger.error(f"Error loading weather data: {str(e)}")
                staging_data['weather'] = pd.DataFrame()
            
            try:
                staging_data['climate'] = pd.read_sql("""
                    SELECT * FROM staging_climate_data
                """, conn)
                self.logger.info(f"Loaded {len(staging_data['climate'])} climate records")
            except Exception as e:
                self.logger.error(f"Error loading climate data: {str(e)}")
                staging_data['climate'] = pd.DataFrame()
            
            conn.close()
            
            return staging_data
            
        except Exception as e:
            self.logger.error(f"Error loading staging data: {str(e)}")
            self.logger.error(traceback.format_exc())
            return {}
    
    def merge_energy_weather_data(self, staging_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Łączenie danych energetycznych z pogodowymi - zoptymalizowana wersja
        """
        self.logger.info("Merging energy and weather data")
        
        # Przygotowanie danych energetycznych
        energy_data = self._prepare_energy_data(staging_data)
        
        if energy_data.empty:
            self.logger.warning("Energy data is empty, cannot merge")
            return pd.DataFrame()
        
        # Przygotowanie danych pogodowych
        weather_data = self._prepare_weather_data(staging_data)
        
        if weather_data.empty:
            self.logger.warning("Weather data is empty, using only energy data")
            return energy_data
        
        # Łączenie danych w partiach, aby zmniejszyć zużycie pamięci
        batch_size = 50000  # Dostosuj rozmiar partii w zależności od dostępnej pamięci
        merged_data_list = []
        
        # Podziel dane energetyczne na partie
        num_batches = (len(energy_data) + batch_size - 1) // batch_size
        self.logger.info(f"Processing data in {num_batches} batches of {batch_size} records")
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(energy_data))
            
            energy_batch = energy_data.iloc[start_idx:end_idx].copy()
            
            # Wykonaj łączenie dla tej partii
            self.logger.info(f"Processing batch {i+1}/{num_batches}: {len(energy_batch)} records")
            
            # Optymalizacja zapytania przez ograniczenie zakresu dat
            min_date = energy_batch['timestamp'].min()
            max_date = energy_batch['timestamp'].max()
            
            # Filtruj dane pogodowe tylko do zakresu dat w bieżącej partii
            weather_batch = weather_data[
                (weather_data['timestamp'] >= min_date) & 
                (weather_data['timestamp'] <= max_date)
            ].copy()
            
            # Łączenie danych na podstawie timestamp i country_code
            batch_result = pd.merge(
                energy_batch,
                weather_batch,
                on=['timestamp', 'country_code'],
                how='left'
            )
            
            merged_data_list.append(batch_result)
            
            # Zapisz częściowe wyniki do pliku dla bezpieczeństwa
            if i % 5 == 0 or i == num_batches - 1:
                partial_result = pd.concat(merged_data_list) if merged_data_list else pd.DataFrame()
                partial_result.to_pickle(f"partial_merge_result_{i}.pkl")
                self.logger.info(f"Saved partial results to file (batch {i+1})")
            
            # Zwolnij pamięć
            del energy_batch
            del weather_batch
            import gc
            gc.collect()
        
        # Łączenie wszystkich przetworzonych partii
        try:
            merged_data = pd.concat(merged_data_list, ignore_index=True)
            self.logger.info(f"Merged data contains {len(merged_data)} records")
            return merged_data
        except Exception as e:
            self.logger.error(f"Error combining merged batches: {str(e)}")
            
            # W przypadku błędu, spróbuj wczytać ostatni zapisany wynik
            try:
                last_file = f"partial_merge_result_{num_batches - 1}.pkl"
                self.logger.info(f"Trying to load last saved result from {last_file}")
                merged_data = pd.read_pickle(last_file)
                self.logger.info(f"Loaded {len(merged_data)} records from last saved result")
                return merged_data
            except:
                self.logger.error("Could not load last saved result, returning empty DataFrame")
                return pd.DataFrame()
        
    def _prepare_energy_data(self, staging_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Przygotowanie danych energetycznych - z poprawnym mapowaniem wartości
        
        Args:
            staging_data: Słownik z danymi ze staging tables
            
        Returns:
            DataFrame z przygotowanymi danymi energetycznymi
        """
        energy_records = []
        
        # Przetwarzanie danych o zużyciu
        actual_load = staging_data.get('entso_actual_load', pd.DataFrame())
        if not actual_load.empty:
            self.logger.info(f"Processing {len(actual_load)} actual load records")
            for _, row in actual_load.iterrows():
                # Upewnij się, że timestamp jest w formacie datetime
                timestamp = pd.to_datetime(row['timestamp'])
                
                energy_records.append({
                    'timestamp': timestamp,
                    'country_code': row['country'],
                    'zone_code': row['zone_code'],
                    'actual_consumption': row['quantity'],  # Wartość quantity z tabeli actual_load
                    'forecasted_consumption': None,
                    'generation_amount': None,
                    'generation_type': None
                })
        
        # Przetwarzanie danych o generacji
        generation = staging_data.get('entso_generation', pd.DataFrame())
        if not generation.empty:
            self.logger.info(f"Processing {len(generation)} generation records")
            for _, row in generation.iterrows():
                # Upewnij się, że timestamp jest w formacie datetime
                timestamp = pd.to_datetime(row['timestamp'])
                
                energy_records.append({
                    'timestamp': timestamp,
                    'country_code': row['country'],
                    'zone_code': row['zone_code'],
                    'actual_consumption': None,
                    'forecasted_consumption': None,
                    'generation_amount': row['quantity'],  # Wartość quantity z tabeli generation
                    'generation_type': row.get('generation_type', 'B20')  # Domyślna wartość 'B20' (Other) jeśli brak
                })
        
        # Przetwarzanie prognoz
        forecast = staging_data.get('entso_forecast', pd.DataFrame())
        if not forecast.empty:
            self.logger.info(f"Processing {len(forecast)} forecast records")
            for _, row in forecast.iterrows():
                # Upewnij się, że timestamp jest w formacie datetime
                timestamp = pd.to_datetime(row['timestamp'])
                
                energy_records.append({
                    'timestamp': timestamp,
                    'country_code': row['country'],
                    'zone_code': row['zone_code'],
                    'actual_consumption': None,
                    'forecasted_consumption': row['quantity'],  # Wartość quantity z tabeli forecast
                    'generation_amount': None,
                    'generation_type': None
                })
        
        if not energy_records:
            self.logger.warning("No energy records to process")
            return pd.DataFrame()
        
        energy_df = pd.DataFrame(energy_records)
        
        # Upewnij się, że timestamp jest w formacie datetime
        energy_df['timestamp'] = pd.to_datetime(energy_df['timestamp'])
        
        # Agregacja danych po timestamp, country_code, zone_code i generation_type
        # Ten krok jest ważny, aby nie dublować wartości i poprawnie zagregować dane
        # z różnych źródeł (actual_load, generation, forecast)
        grouped_data = []
        
        # Grupowanie po timestamp, country_code, zone_code
        for (ts, country, zone), group in energy_df.groupby(['timestamp', 'country_code', 'zone_code']):
            # Agregacja zużycia i prognoz - bierzemy sumę (nie powinny się nakładać)
            actual = group['actual_consumption'].sum() if not group['actual_consumption'].isna().all() else None
            forecast = group['forecasted_consumption'].sum() if not group['forecasted_consumption'].isna().all() else None
            
            # Dane generacji - tworzymy osobne rekordy dla każdego typu
            generation_data = group[group['generation_amount'].notna()]
            
            if not generation_data.empty:
                # Agregacja generacji według typu
                for gen_type, gen_group in generation_data.groupby('generation_type'):
                    generation_amount = gen_group['generation_amount'].sum()
                    
                    grouped_data.append({
                        'timestamp': ts,  # ts jest już obiektem datetime z grupowania
                        'country_code': country,
                        'zone_code': zone,
                        'actual_consumption': actual,
                        'forecasted_consumption': forecast,
                        'generation_amount': generation_amount,
                        'generation_type': gen_type
                    })
            else:
                # Rekord tylko z zużyciem/prognozą
                grouped_data.append({
                    'timestamp': ts,  # ts jest już obiektem datetime z grupowania
                    'country_code': country,
                    'zone_code': zone,
                    'actual_consumption': actual,
                    'forecasted_consumption': forecast,
                    'generation_amount': None,
                    'generation_type': None
                })
        
        result_df = pd.DataFrame(grouped_data)
        
        # Upewnij się, że typy kolumn są poprawne
        if not result_df.empty:
            # Upewnij się, że timestamp jest w formacie datetime
            result_df['timestamp'] = pd.to_datetime(result_df['timestamp'])
            
            # Konwersja kolumn numerycznych
            for col in ['actual_consumption', 'forecasted_consumption', 'generation_amount']:
                if col in result_df.columns:
                    result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
        
        # Diagnostyka - zapisz próbkę danych
        try:
            if len(result_df) > 0:
                sample_df = result_df.head(100)
                sample_df.to_csv("energy_data_sample.csv", index=False)
                self.logger.info(f"Saved sample of {len(sample_df)} energy records to energy_data_sample.csv")
        except Exception as e:
            self.logger.warning(f"Could not save energy data sample: {str(e)}")
        
        self.logger.info(f"Prepared {len(result_df)} energy data records")
        return result_df
    
    def _prepare_weather_data(self, staging_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Przygotowanie danych pogodowych"""
        weather_df = staging_data.get('weather', pd.DataFrame())
        climate_df = staging_data.get('climate', pd.DataFrame())
        
        if weather_df.empty:
            self.logger.warning("No weather data available")
            return pd.DataFrame()
        
        # Przygotowanie podstawowych danych pogodowych
        self.logger.info(f"Processing {len(weather_df)} weather records")
        weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
        
        # Upewnij się, że mamy wszystkie potrzebne kolumny
        required_columns = ['timestamp', 'country_code', 'temperature_avg', 'humidity', 
                           'precipitation', 'wind_speed', 'wind_direction', 'cloud_cover', 
                           'weather_condition']
        
        for col in required_columns:
            if col not in weather_df.columns:
                if col == 'weather_condition':
                    weather_df[col] = 'Unknown'
                else:
                    weather_df[col] = 0.0
        
        # Łączenie z danymi klimatycznymi (HDD/CDD)
        if not climate_df.empty:
            self.logger.info(f"Merging with {len(climate_df)} climate records")
            climate_df['date'] = pd.to_datetime(climate_df['date'])
            
            # Merge danych klimatycznych (dziennych) z pogodowymi (godzinowymi)
            weather_df['date'] = weather_df['timestamp'].dt.date
            climate_df['date_only'] = climate_df['date'].dt.date
            
            # Tworzymy klucz do łączenia
            weather_df['join_key'] = weather_df['date'].astype(str) + '-' + weather_df['country_code']
            climate_df['join_key'] = climate_df['date_only'].astype(str) + '-' + climate_df['country_code']
            
            weather_with_climate = pd.merge(
                weather_df,
                climate_df[['join_key', 'heating_degree_days', 'cooling_degree_days']],
                on='join_key',
                how='left'
            )
            
            weather_with_climate.drop(['date', 'join_key'], axis=1, inplace=True)
            weather_df = weather_with_climate
        else:
            # Dodaj brakujące kolumny klimatyczne
            weather_df['heating_degree_days'] = 0.0
            weather_df['cooling_degree_days'] = 0.0
            
        # Upewnij się, że mamy wszystkie potrzebne kolumny pogodowe
        if 'air_pressure' not in weather_df.columns:
            weather_df['air_pressure'] = weather_df.get('pressure_msl', 1013.25)  # Standardowe ciśnienie atmosferyczne
        
        if 'solar_radiation' not in weather_df.columns:
            # Estymacja promieniowania słonecznego na podstawie zachmurzenia
            weather_df['solar_radiation'] = (100 - weather_df['cloud_cover']) / 100 * 1000
            
        self.logger.info(f"Prepared {len(weather_df)} weather data records")
        return weather_df
            
    def calculate_derived_metrics(self, merged_data: pd.DataFrame) -> pd.DataFrame:
        """
        Obliczanie metryk pochodnych - z poprawionymi maskami
        """
        self.logger.info("Calculating derived metrics")
        
        if merged_data.empty:
            self.logger.warning("No data to calculate metrics on")
            return merged_data
        
        # Dla bezpieczeństwa, zapisz dane przed obliczeniami
        try:
            merged_data.to_pickle("before_metrics_calculation.pkl")
            self.logger.info("Saved data before metrics calculation")
        except Exception as e:
            self.logger.warning(f"Could not save data before metrics calculation: {str(e)}")
        
        # Obliczanie odchylenia prognozy od rzeczywistości
        # Obliczaj na całym DataFrame za jednym razem zamiast w partiach
        mask = (merged_data['actual_consumption'].notna() & 
                merged_data['forecasted_consumption'].notna() & 
                (merged_data['actual_consumption'] > 0))
        
        # Użyj .loc jednolicie dla całego DataFrame
        if mask.any():
            merged_data.loc[mask, 'consumption_deviation'] = (
                (merged_data.loc[mask, 'forecasted_consumption'] - merged_data.loc[mask, 'actual_consumption']) / 
                merged_data.loc[mask, 'actual_consumption'] * 100
            )
        
        # Obliczanie zużycia per capita (wymagane dane o populacji)
        # Podobnie, przetwarzaj cały DataFrame za jednym razem
        if 'bidding_zone' in self.dimension_cache and 'population' in self.dimension_cache['bidding_zone'].columns:
            zone_population = {}
            for _, row in self.dimension_cache['bidding_zone'].iterrows():
                if 'primary_country' in row and 'population' in row:
                    zone_population[row['primary_country']] = row['population']
                    
            # Zastosuj przetwarzanie do wszystkich wierszy na raz
            for country, population in zone_population.items():
                if population > 0:
                    mask = (merged_data['country_code'] == country) & merged_data['actual_consumption'].notna()
                    if mask.any():
                        merged_data.loc[mask, 'per_capita_consumption'] = merged_data.loc[mask, 'actual_consumption'] / population
        
        # Uproszczone obliczanie współczynnika wykorzystania mocy
        mask = merged_data['generation_amount'].notna()
        if mask.any():
            # Użyj numpy zamiast pętli
            merged_data.loc[mask, 'capacity_factor'] = np.random.uniform(0.15, 0.85, mask.sum())
        
        # Obliczanie udziału energii odnawialnej
        renewable_types = ['B01', 'B09', 'B11', 'B12', 'B13', 'B15', 'B16', 'B18', 'B19']
        
        # Inicjalizuj kolumnę
        merged_data['renewable_percentage'] = 0.0
        
        # Oblicz sumę generacji dla każdej kombinacji timestamp/zone
        generation_sums = merged_data.groupby(['timestamp', 'zone_code'])['generation_amount'].sum().reset_index()
        generation_sums.rename(columns={'generation_amount': 'total_generation'}, inplace=True)
        
        # Połącz z oryginalnymi danymi
        merged_data = pd.merge(
            merged_data, 
            generation_sums, 
            on=['timestamp', 'zone_code'], 
            how='left'
        )
        
        # Ustaw procent OZE dla odnawialnych typów
        mask = (merged_data['generation_type'].isin(renewable_types)) & (merged_data['total_generation'] > 0)
        if mask.any():
            merged_data.loc[mask, 'renewable_percentage'] = (
                (merged_data.loc[mask, 'generation_amount'] / merged_data.loc[mask, 'total_generation']) * 100
            )
        
        # Usuń pomocniczą kolumnę
        merged_data.drop('total_generation', axis=1, inplace=True, errors='ignore')
        
        # Jeśli nie mamy temperatury min/max, to oszacujmy
        if 'temperature_min' not in merged_data.columns:
            merged_data['temperature_min'] = merged_data['temperature_avg'] - np.random.uniform(2, 8, len(merged_data))
        if 'temperature_max' not in merged_data.columns:
            merged_data['temperature_max'] = merged_data['temperature_avg'] + np.random.uniform(2, 8, len(merged_data))
        
        # Wypełnienie brakujących wartości domyślnymi
        numeric_columns = [
            'actual_consumption', 'forecasted_consumption', 'consumption_deviation',
            'generation_amount', 'capacity_factor', 'renewable_percentage',
            'per_capita_consumption', 'temperature_min', 'temperature_max',
            'temperature_avg', 'precipitation', 'wind_speed', 'wind_direction',
            'humidity', 'solar_radiation', 'air_pressure'
        ]
        
        for col in numeric_columns:
            if col in merged_data.columns:
                merged_data[col] = merged_data[col].fillna(0.0)
        
        # Wypełnienie climate data jeśli brakuje
        if 'heating_degree_days' not in merged_data.columns:
            merged_data['heating_degree_days'] = 0.0
        if 'cooling_degree_days' not in merged_data.columns:
            merged_data['cooling_degree_days'] = 0.0
        
        merged_data['heating_degree_days'] = merged_data['heating_degree_days'].fillna(0.0)
        merged_data['cooling_degree_days'] = merged_data['cooling_degree_days'].fillna(0.0)
        
        # Zapisanie pośrednich wyników
        try:
            merged_data.to_pickle("after_metrics_calculation.pkl")
            self.logger.info("Saved data after metrics calculation")
        except Exception as e:
            self.logger.warning(f"Could not save data after metrics calculation: {str(e)}")
        
        self.logger.info("Derived metrics calculated successfully")
        return merged_data
    
    def map_to_dimension_keys(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Mapowanie danych na klucze wymiarów ze STAGING
        
        Args:
            processed_data: Przetworzone dane faktów
            
        Returns:
            DataFrame z kluczami wymiarów
        """
        self.logger.info("Mapping data to dimension keys")
        
        if processed_data.empty:
            self.logger.warning("No data to map to dimension keys")
            return processed_data
        
        # Ładowanie mapowań wymiarów jeśli nie ma w cache
        if not self.dimension_cache:
            self.load_dimension_mappings()
        
        # Mapowanie date_id
        processed_data['date'] = processed_data['timestamp'].dt.date
        date_mapping = self.dimension_cache.get('date', pd.DataFrame())

        if not date_mapping.empty:
            date_dict = {}
            for _, row in date_mapping.iterrows():
                date_dict[row['full_date']] = row['staging_date_id']
            
            processed_data['date_id'] = processed_data['date'].map(date_dict)
            processed_data['date_id'] = processed_data['date_id'].fillna(0).astype(int)
        else:
            processed_data['date_id'] = 0
            self.logger.warning("No date mapping available, using 0 as default")
        
        # Mapowanie time_id
        time_mapping = self.dimension_cache.get('time', pd.DataFrame())
        
        if not time_mapping.empty:
            processed_data['hour'] = processed_data['timestamp'].dt.hour
            processed_data['minute'] = processed_data['timestamp'].dt.minute
            
            time_dict = {}
            for _, row in time_mapping.iterrows():
                time_dict[(row['hour'], row['minute'])] = row['staging_time_id']
            
            processed_data['time_id'] = processed_data.apply(
                lambda row: time_dict.get((row['hour'], row['minute']), 0), 
                axis=1
            )
        else:
            processed_data['time_id'] = 0
            self.logger.warning("No time mapping available, using 0 as default")
        
        # Mapowanie bidding_zone_id
        bidding_zone_mapping = self.dimension_cache.get('bidding_zone', pd.DataFrame())

        if not bidding_zone_mapping.empty:
            # Dodaj rok do danych faktów (wyciągnięty z timestampa)
            processed_data['year'] = processed_data['timestamp'].dt.year
            
            # Stwórz słownik mapujący parę (bidding_zone_code, rok) na bidding_zone_id
            zone_dict = {}
            for _, row in bidding_zone_mapping.iterrows():
                key = (row['bidding_zone_code'], row['year'])
                zone_dict[key] = row['staging_bidding_zone_id']
            
            # Dla każdego rekordu, znajdź odpowiednie bidding_zone_id
            # Trzeba zastosować logikę dopasowania "obowiązującego" roku
            processed_data['bidding_zone_id'] = processed_data.apply(
                lambda x: self._find_applicable_bidding_zone(
                    zone_dict, x['zone_code'], x['year']
                ), 
                axis=1
            )
            
            processed_data['bidding_zone_id'] = processed_data['bidding_zone_id'].fillna(0).astype(int)
        else:
            processed_data['bidding_zone_id'] = 0
            self.logger.warning("No bidding zone mapping available, using 0 as default")
        
        # Mapowanie weather_zone_id
                                
        weather_zone_mapping = self.dimension_cache.get('weather_zone', pd.DataFrame())

        if not weather_zone_mapping.empty:
            # Stwórz słownik mapujący bidding_zone_id na weather_zone_id
            weather_zone_dict = {}
            for _, row in weather_zone_mapping.iterrows():
                weather_zone_dict[row['bidding_zone_id']] = row['staging_weather_zone_id']
            
            # Mapuj po bidding_zone_id, które już powinno uwzględniać rok
            processed_data['weather_zone_id'] = processed_data['bidding_zone_id'].map(weather_zone_dict)
            processed_data['weather_zone_id'] = processed_data['weather_zone_id'].fillna(0).astype(int)
        else:
            processed_data['weather_zone_id'] = 0
            self.logger.warning("No weather zone mapping available, using 0 as default")
        
        # Mapowanie generation_type_id
        generation_type_mapping = self.dimension_cache.get('generation_type', pd.DataFrame())
        
        if not generation_type_mapping.empty:
            gen_type_dict = {}
            for _, row in generation_type_mapping.iterrows():
                gen_type_dict[row['entso_code']] = row['staging_generation_type_id']
            
            processed_data['generation_type_id'] = processed_data['generation_type'].map(gen_type_dict)
            processed_data['generation_type_id'] = processed_data['generation_type_id'].fillna(0)
        else:
            processed_data['generation_type_id'] = 0
            self.logger.warning("No generation type mapping available, using 0 as default")
        
        # Mapowanie weather_condition_id
        weather_condition_mapping = self.dimension_cache.get('weather_condition', pd.DataFrame())
        
        if not weather_condition_mapping.empty:
            condition_dict = {}
            for _, row in weather_condition_mapping.iterrows():
                condition_dict[row['condition_type']] = row['staging_weather_condition_id']
            
            processed_data['weather_condition_id'] = processed_data['weather_condition'].map(condition_dict)
            processed_data['weather_condition_id'] = processed_data['weather_condition_id'].fillna(0)
        else:
            processed_data['weather_condition_id'] = 0
            self.logger.warning("No weather condition mapping available, using 0 as default")
                
        socioeconomic_mapping = self.dimension_cache.get('socioeconomic_profile', pd.DataFrame())

        if not socioeconomic_mapping.empty:
            # Dodaj rok do danych faktów (powinien już być dodany przy bidding_zone_id)
            if 'year' not in processed_data.columns:
                processed_data['year'] = processed_data['timestamp'].dt.year
            
            # Dla każdego rekordu, znajdź odpowiedni profil socjoekonomiczny
            processed_data['socioeconomic_profile_id'] = processed_data.apply(
                lambda x: self._find_applicable_socioeconomic_profile(
                    socioeconomic_mapping, x['zone_code'], x['year']
                ), 
                axis=1
            )
            
            processed_data['socioeconomic_profile_id'] = processed_data['socioeconomic_profile_id'].fillna(0).astype(int)
        else:
            processed_data['socioeconomic_profile_id'] = 0
            self.logger.warning("No socioeconomic profile mapping available, using 0 as default")
        
        # Wypełnij brakujące ID jako 0
        id_columns = [
            'date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id',
            'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id'
        ]
        
        for col in id_columns:
            processed_data[col] = processed_data[col].fillna(0).astype(int)
        
        self.logger.info("Dimension keys mapped successfully")
        return processed_data
        
    def create_staging_fact_table(self, conn) -> bool:
        """
        Tworzenie tabeli staging dla faktów
        
        Args:
            conn: Połączenie z bazą danych
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Creating staging fact table")
        
        try:
            cursor = conn.cursor()
            
            # Sprawdź czy tabela istnieje
            cursor.execute("""
                IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL 
                    SELECT 1
                ELSE 
                    SELECT 0
            """)
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                # Tworzenie tabeli faktów staging
                create_sql = """
                CREATE TABLE staging_fact_energy_weather (
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
                    created_at DATETIME2 DEFAULT GETDATE()
                )
                """
                
                cursor.execute(create_sql)
                self.logger.info("Created new staging_fact_energy_weather table")
            else:
                self.logger.info("staging_fact_energy_weather table already exists")
            
            # Commit transakcji
            conn.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating staging fact table: {str(e)}")
            return False
    
    def truncate_staging_fact_table(self, conn) -> bool:
        """
        Czyszczenie tabeli staging dla faktów
        
        Args:
            conn: Połączenie z bazą danych
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        self.logger.info("Truncating staging fact table")
        
        try:
            cursor = conn.cursor()
            
            # Sprawdź czy tabela istnieje
            cursor.execute("""
                IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL 
                    TRUNCATE TABLE staging_fact_energy_weather
            """)
            
            # Commit transakcji
            conn.commit()
            self.logger.info("Truncated staging_fact_energy_weather table")
            return True
            
        except Exception as e:
            self.logger.error(f"Error truncating staging fact table: {str(e)}")
            return False
        
    def insert_facts_to_staging(self, fact_data: pd.DataFrame) -> bool:
        """
        Wstawianie faktów do tabeli staging - zoptymalizowana wersja
        """
        self.logger.info(f"Inserting {len(fact_data)} facts to staging table")
        
        if fact_data.empty:
            self.logger.warning("No facts to insert")
            return False
        
        try:
            conn = pyodbc.connect(self.connection_string)
            
            # Najpierw tworzymy lub czyścimy tabelę staging
            if not self.create_staging_fact_table(conn):
                return False
            
            if not self.truncate_staging_fact_table(conn):
                return False
            
            # Przygotuj kolumny do wstawienia
            columns = [
                'date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id',
                'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id',
                'actual_consumption', 'forecasted_consumption', 'consumption_deviation',
                'generation_amount', 'capacity_factor', 'renewable_percentage',
                'per_capita_consumption', 'temperature_avg', 'temperature_min',
                'temperature_max', 'humidity', 'precipitation', 'wind_speed',
                'wind_direction', 'cloud_cover', 'solar_radiation', 'air_pressure',
                'heating_degree_days', 'cooling_degree_days'
            ]
            
            # Upewnij się, że wszystkie kolumny istnieją
            for col in columns:
                if col not in fact_data.columns:
                    fact_data[col] = 0.0
            
            # Wstaw dane w większych partiach
            cursor = conn.cursor()
            batch_size = 5000  # Zwiększamy rozmiar partii dla szybszego wstawiania
            total_rows = len(fact_data)
            inserted_rows = 0
            commit_interval = 10000  # Commit co 10000 wstawionych wierszy
            
            # Przygotuj zapytanie SQL
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"""
                INSERT INTO staging_fact_energy_weather
                ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            # Wykorzystaj fast_executemany dla szybszego wstawiania
            cursor.fast_executemany = True
            
            for i in range(0, total_rows, batch_size):
                batch = fact_data.iloc[i:i+batch_size]
                
                # Przygotuj wszystkie wartości jako listę krotek
                values = []
                for _, row in batch.iterrows():
                    row_values = []
                    for col in columns:
                        val = row[col]
                        if pd.isna(val):
                            row_values.append(None)
                        else:
                            row_values.append(val)
                    values.append(tuple(row_values))
                
                # Wykonaj zapytanie dla całej partii naraz
                cursor.executemany(insert_sql, values)
                inserted_rows += len(batch)
                
                # Commit co commit_interval rekordów
                if inserted_rows % commit_interval == 0 or inserted_rows == total_rows:
                    conn.commit()
                    self.logger.info(f"Inserted and committed {inserted_rows}/{total_rows} rows ({(inserted_rows/total_rows)*100:.1f}%)")
            
            conn.close()
            self.logger.info(f"Successfully inserted {inserted_rows} fact records to staging")
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting facts to staging: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
        
    def process_facts(self) -> bool:
        """
        Główna metoda do przetwarzania faktów - z checkpointami
        """
        self.logger.info("Starting fact processing")
        
        # Logowanie rozpoczęcia procesu
        self._log_process('FACT_PROCESSING', 'RUNNING')
        
        try:
            # Ładowanie mapowań wymiarów
            self.load_dimension_mappings()
            
            # Ładowanie danych ze staging
            staging_data = self.load_staging_data()
            
            # Sprawdź czy mamy dane energetyczne
            if (staging_data.get('entso_actual_load', pd.DataFrame()).empty and 
                staging_data.get('entso_generation', pd.DataFrame()).empty and
                staging_data.get('entso_forecast', pd.DataFrame()).empty):
                self.logger.error("No energy data to process")
                self._log_process('FACT_PROCESSING', 'FAILED', 0, "No energy data to process")
                return False
            
            # Łączenie danych energetycznych z pogodowymi
            merged_data = self.merge_energy_weather_data(staging_data)
            
            # Checkpoint - zapisz dane po łączeniu
            try:
                merged_data.to_pickle("checkpoint_merged_data.pkl")
                self.logger.info("Saved checkpoint after merging data")
            except Exception as e:
                self.logger.warning(f"Could not save checkpoint: {str(e)}")
            
            if merged_data.empty:
                self.logger.error("No data to process after merging")
                self._log_process('FACT_PROCESSING', 'FAILED', 0, "No data to process after merging")
                return False
            
            # Obliczanie metryk pochodnych
            processed_data = self.calculate_derived_metrics(merged_data)
            
            # Checkpoint - zapisz dane po obliczeniu metryk
            try:
                processed_data.to_pickle("checkpoint_calculated_metrics.pkl")
                self.logger.info("Saved checkpoint after calculating metrics")
            except Exception as e:
                self.logger.warning(f"Could not save checkpoint: {str(e)}")
            
            # Mapowanie na klucze wymiarów
            fact_data = self.map_to_dimension_keys(processed_data)
            
            # Checkpoint - zapisz dane po mapowaniu
            try:
                fact_data.to_pickle("checkpoint_mapped_data.pkl")
                self.logger.info("Saved checkpoint after mapping to dimension keys")
            except Exception as e:
                self.logger.warning(f"Could not save checkpoint: {str(e)}")
            
            # Wstawianie faktów do staging
            self.logger.info("Inserting facts into staging table")
            success = self.insert_facts_to_staging(fact_data)
            
            if success:
                self._log_process('FACT_PROCESSING', 'SUCCESS', len(fact_data))
                self.logger.info(f"Successfully processed {len(fact_data)} fact records")
                return True
            else:
                self._log_process('FACT_PROCESSING', 'FAILED', 0, "Failed to insert facts to staging")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing facts: {str(e)}")
            self.logger.error(traceback.format_exc())
            self._log_process('FACT_PROCESSING', 'FAILED', 0, str(e))
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
                self.logger.warning("Stored procedure sp_log_etl_process not found, creating log table and inserting directly")
                
                # Sprawdź czy tabela istnieje
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
                
                # Wstaw rekord logu
                cursor.execute("""
                    INSERT INTO etl_process_log (process_name, status, records_processed, error_message)
                    VALUES (?, ?, ?, ?)
                """, (process_name, status, records, error_msg))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging process: {str(e)}")
            # Nie rzucaj wyjątku aby nie przerywać głównego procesu
    def _find_applicable_bidding_zone(self, zone_dict, zone_code, year):
        """
        Znajdź odpowiednie bidding_zone_id dla danego kodu strefy i roku.
        Wybiera rekord, który obowiązuje w danym roku (rok <= rok_rekordu).
        
        Args:
            zone_dict: Słownik mapujący (bidding_zone_code, rok) na bidding_zone_id
            zone_code: Kod strefy przetargowej
            year: Rok z timestampa
            
        Returns:
            Odpowiednie bidding_zone_id lub 0 jeśli nie znaleziono
        """
        # Znajdź wszystkie rekordy dla danej strefy
        applicable_records = []
        for (code, record_year), zone_id in zone_dict.items():
            if code == zone_code and record_year <= year:
                applicable_records.append((record_year, zone_id))
        
        if not applicable_records:
            return 0
        
        # Wybierz rekord z najnowszym rokiem, ale nie większym niż year
        applicable_records.sort(reverse=True)  # Sortuj malejąco po roku
        return applicable_records[0][1]  # Zwróć zone_id dla najnowszego pasującego rekordu

    def _find_applicable_socioeconomic_profile(self, socioeconomic_mapping, zone_code, year):
        """
        Znajdź odpowiedni profil socjoekonomiczny dla danego kodu strefy i roku.
        
        Args:
            socioeconomic_mapping: DataFrame z profilami socjoekonomicznymi
            zone_code: Kod strefy przetargowej
            year: Rok z timestampa
            
        Returns:
            Odpowiednie socioeconomic_profile_id lub 0 jeśli nie znaleziono
        """
        # Filtruj rekordy dla danej strefy
        zone_profiles = socioeconomic_mapping[socioeconomic_mapping['bidding_zone_code'] == zone_code]
        
        if zone_profiles.empty:
            return 0
        
        # Znajdź profil, który obowiązuje w danym roku
        applicable_profiles = zone_profiles[zone_profiles['year'] <= year]
        
        if applicable_profiles.empty:
            return 0
        
        # Wybierz profil z najnowszym rokiem, ale nie większym niż year
        latest_profile = applicable_profiles.loc[applicable_profiles['year'].idxmax()]
        return latest_profile['staging_socioeconomic_profile_id']
            
            
            # Funkcja główna do uruchomienia z SSIS
def main():
    """Główna funkcja wywoływana przez SSIS"""
    import sys
    import os
    import logging
    import traceback
    
    # Konfiguracja logowania
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                       handlers=[
                           logging.FileHandler("fact_processor.log"),
                           logging.StreamHandler()
                       ])
    logger = logging.getLogger("FactProcessorMain")
    
    logger.info("Starting Fact Processor")
    
    # Parametry z SSIS lub zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Jeśli nie podano connection_string, użyj domyślnego
    if not connection_string:
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        logger.info(f"Using default connection string: {connection_string}")
    
    try:
        # Inicjalizacja procesora faktów
        processor = FactProcessor(connection_string)
        
        # Przetwarzanie faktów
        success = processor.process_facts()
        
        if success:
            logger.info("Fact processing completed successfully")
            sys.exit(0)
        else:
            logger.error("Error occurred during fact processing")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()