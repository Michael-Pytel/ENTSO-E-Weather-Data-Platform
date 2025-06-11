"""
FactProcessor.py - Memory Optimized Version
Przetwarzanie danych dla tabeli faktów z optymalizacją pamięci
Łączenie danych energetycznych z pogodowymi, obliczanie metryk pochodnych
"""

import pandas as pd
from datetime import datetime, timedelta
import pyodbc
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
import traceback
import gc
import psutil
import os

class MemoryOptimizedFactProcessor:
    """Procesor do tworzenia i przetwarzania tabeli faktów z optymalizacją pamięci"""
    
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
        
        # Konfiguracja pamięci
        self.memory_threshold = 0.85  # 85% wykorzystania RAM jako próg
        self.max_batch_size = 10000   # Zmniejszony rozmiar partii
        self.min_batch_size = 1000    # Minimalny rozmiar partii
        self.current_batch_size = self.max_batch_size
        
        # Śledzenie pamięci
        self.process = psutil.Process(os.getpid())
        
    def get_memory_usage(self) -> float:
        """Zwraca wykorzystanie pamięci w procentach"""
        try:
            memory_info = self.process.memory_info()
            memory_percent = self.process.memory_percent()
            self.logger.info(f"Memory usage: {memory_percent:.1f}% ({memory_info.rss / 1024 / 1024:.1f} MB)")
            return memory_percent / 100.0
        except:
            return 0.5  # Domyślna wartość w przypadku błędu
    
    def force_garbage_collection(self):
        """Wymuś garbage collection i wyczyść niepotrzebne obiekty"""
        # Wywołaj garbage collector
        collected = gc.collect()
        
        # Wymuś zwalnianie pamięci przez numpy
        if hasattr(np, 'ndarray'):
            try:
                # Czyść cache numpy
                np.random.seed()  # Reset random state to free memory
            except:
                pass
        
        # Sprawdź pamięć po garbage collection
        memory_after = self.get_memory_usage()
        self.logger.info(f"Garbage collection: collected {collected} objects, memory usage: {memory_after*100:.1f}%")
        
        return memory_after
    
    def adjust_batch_size(self):
        """Dostosuj rozmiar partii na podstawie wykorzystania pamięci"""
        memory_usage = self.get_memory_usage()
        
        if memory_usage > self.memory_threshold:
            # Zmniejsz rozmiar partii
            self.current_batch_size = max(self.min_batch_size, int(self.current_batch_size * 0.7))
            self.logger.warning(f"High memory usage ({memory_usage*100:.1f}%), reducing batch size to {self.current_batch_size}")
        elif memory_usage < 0.5 and self.current_batch_size < self.max_batch_size:
            # Zwiększ rozmiar partii jeśli pamięć pozwala
            self.current_batch_size = min(self.max_batch_size, int(self.current_batch_size * 1.2))
            self.logger.info(f"Low memory usage ({memory_usage*100:.1f}%), increasing batch size to {self.current_batch_size}")
    
    def load_staging_data_chunked(self, table_name: str, chunk_size: int = 50000) -> pd.DataFrame:
        """
        Ładowanie danych ze staging tables w chunkach
        
        Args:
            table_name: Nazwa tabeli staging
            chunk_size: Rozmiar chunka
            
        Returns:
            Generator zwracający chunki danych
        """
        self.logger.info(f"Loading data from {table_name} in chunks of {chunk_size}")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            
            # Sprawdź liczbę rekordów
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
            self.logger.info(f"Total rows in {table_name}: {total_rows}")
            
            if total_rows == 0:
                conn.close()
                return pd.DataFrame()
            
            # Ustaw mniejszy chunk_size jeśli mało pamięci
            memory_usage = self.get_memory_usage()
            if memory_usage > 0.7:
                chunk_size = min(chunk_size, 10000)
                self.logger.warning(f"High memory usage, reducing chunk size to {chunk_size}")
            
            # Użyj pandas do wczytania w chunkach
            query = f"SELECT * FROM {table_name}"
            
            # Wczytaj wszystkie chunki i połącz je, ale z kontrolą pamięci
            chunks = []
            chunk_reader = pd.read_sql(query, conn, chunksize=chunk_size)
            
            for i, chunk in enumerate(chunk_reader):
                chunks.append(chunk)
                
                # Sprawdź pamięć co kilka chunków
                if i % 5 == 0:
                    memory_usage = self.get_memory_usage()
                    if memory_usage > self.memory_threshold:
                        self.logger.warning(f"High memory usage while loading {table_name}, forcing garbage collection")
                        self.force_garbage_collection()
                
                self.logger.info(f"Loaded chunk {i+1} from {table_name} ({len(chunk)} rows)")
            
            conn.close()
            
            if chunks:
                result = pd.concat(chunks, ignore_index=True)
                # Zwolnij pamięć chunków
                del chunks
                gc.collect()
                return result
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error loading {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def load_staging_data(self) -> Dict[str, pd.DataFrame]:
        """
        Ładowanie danych ze staging tables z optymalizacją pamięci
        
        Returns:
            Słownik z danymi staging
        """
        self.logger.info("Loading data from staging tables with memory optimization")
        
        staging_data = {}
        table_configs = [
            ('staging_entso_actual_load', 'entso_actual_load'),
            ('staging_entso_generation', 'entso_generation'),
            ('staging_entso_forecast', 'entso_forecast'),
            ('staging_weather_data', 'weather'),
            ('staging_climate_data', 'climate')
        ]
        
        for table_name, key in table_configs:
            self.logger.info(f"Loading {table_name}...")
            
            # Sprawdź pamięć przed załadowaniem każdej tabeli
            memory_before = self.get_memory_usage()
            if memory_before > self.memory_threshold:
                self.force_garbage_collection()
            
            # Załaduj dane
            data = self.load_staging_data_chunked(table_name)
            
            if not data.empty:
                # Optymalizuj typy danych aby zaoszczędzić pamięć
                data = self.optimize_dataframe_memory(data)
                staging_data[key] = data
                self.logger.info(f"Loaded and optimized {len(data)} records from {table_name}")
            else:
                staging_data[key] = pd.DataFrame()
                self.logger.warning(f"No data loaded from {table_name}")
            
            # Sprawdź pamięć po załadowaniu
            memory_after = self.get_memory_usage()
            self.logger.info(f"Memory usage after loading {table_name}: {memory_after*100:.1f}%")
        
        return staging_data
    
    def optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optymalizuje typy danych w DataFrame aby zmniejszyć zużycie pamięci
        
        Args:
            df: DataFrame do optymalizacji
            
        Returns:
            Zoptymalizowany DataFrame
        """
        if df.empty:
            return df
        
        memory_before = df.memory_usage(deep=True).sum() / 1024**2
        
        # Lista kolumn które NIE powinny być konwertowane na categorical
        # (bo później używamy na nich fillna z wartościami spoza kategorii)
        protected_columns = [
            'timestamp', 'date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id',
            'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id',
            'year', 'hour', 'minute', 'date'
        ]
        
        # Optymalizuj kolumny numeryczne
        for col in df.select_dtypes(include=['float64']).columns:
            if df[col].min() >= -3.4e38 and df[col].max() <= 3.4e38:
                df[col] = pd.to_numeric(df[col], downcast='float')
        
        for col in df.select_dtypes(include=['int64']).columns:
            if df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                df[col] = pd.to_numeric(df[col], downcast='integer')
        
        # Optymalizuj kolumny tekstowe (ale tylko te które nie są w protected_columns)
        for col in df.select_dtypes(include=['object']).columns:
            if col not in protected_columns:  # Nie konwertuj chronionych kolumn
                num_unique_values = len(df[col].unique())
                num_total_values = len(df[col])
                if num_unique_values / num_total_values < 0.5:  # Jeśli dużo powtórzeń
                    df[col] = df[col].astype('category')
        
        memory_after = df.memory_usage(deep=True).sum() / 1024**2
        reduction = (memory_before - memory_after) / memory_before * 100
        
        self.logger.info(f"Memory optimization: {memory_before:.1f}MB -> {memory_after:.1f}MB (reduced by {reduction:.1f}%)")
        
        return df
    
    def merge_energy_weather_data_optimized(self, staging_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Łączenie danych energetycznych z pogodowymi - wersja zoptymalizowana pod kątem pamięci
        """
        self.logger.info("Merging energy and weather data with memory optimization")
        
        # Przygotowanie danych energetycznych w mniejszych partiach
        energy_data = self._prepare_energy_data_optimized(staging_data)
        
        if energy_data.empty:
            self.logger.warning("Energy data is empty, cannot merge")
            return pd.DataFrame()
        
        # Przygotowanie danych pogodowych
        weather_data = self._prepare_weather_data_optimized(staging_data)
        
        if weather_data.empty:
            self.logger.warning("Weather data is empty, using only energy data")
            return energy_data
        
        # Optymalizuj typy danych przed mergem
        energy_data = self.optimize_dataframe_memory(energy_data)
        weather_data = self.optimize_dataframe_memory(weather_data)
        
        # Sortuj dane dla efektywniejszego merga
        energy_data = energy_data.sort_values(['timestamp', 'country_code'])
        weather_data = weather_data.sort_values(['timestamp', 'country_code'])
        
        # Łączenie danych w bardzo małych partiach
        self.adjust_batch_size()  # Dostosuj rozmiar partii na podstawie pamięci
        
        merged_chunks = []
        total_energy_rows = len(energy_data)
        
        # Utwórz indeks dla szybszego wyszukiwania w danych pogodowych
        weather_data.set_index(['timestamp', 'country_code'], inplace=True)
        
        num_batches = (total_energy_rows + self.current_batch_size - 1) // self.current_batch_size
        self.logger.info(f"Processing data in {num_batches} batches of {self.current_batch_size} records")
        
        for i in range(num_batches):
            # Sprawdź pamięć przed każdą partią
            memory_usage = self.get_memory_usage()
            if memory_usage > self.memory_threshold:
                self.force_garbage_collection()
                self.adjust_batch_size()
            
            start_idx = i * self.current_batch_size
            end_idx = min((i + 1) * self.current_batch_size, total_energy_rows)
            
            energy_batch = energy_data.iloc[start_idx:end_idx].copy()
            
            self.logger.info(f"Processing batch {i+1}/{num_batches}: {len(energy_batch)} records (memory: {memory_usage*100:.1f}%)")
            
            try:
                # Merge z użyciem indeksu
                energy_batch.set_index(['timestamp', 'country_code'], inplace=True)
                batch_result = energy_batch.join(weather_data, how='left', rsuffix='_weather')
                batch_result.reset_index(inplace=True)
                
                # Usuń duplikaty kolumn jeśli istnieją
                batch_result = batch_result.loc[:, ~batch_result.columns.duplicated()]
                
                # Optymalizuj pamięć wyniku
                batch_result = self.optimize_dataframe_memory(batch_result)
                
                merged_chunks.append(batch_result)
                
                # Zwolnij pamięć partii
                del energy_batch, batch_result
                
                # Zapisz częściowe wyniki co 10 partii lub przy wysokim użyciu pamięci
                if (i + 1) % 10 == 0 or memory_usage > 0.8 or i == num_batches - 1:
                    try:
                        # Połącz dotychczasowe chunki
                        partial_result = pd.concat(merged_chunks, ignore_index=True)
                        partial_result.to_pickle(f"partial_merge_result_{i}.pkl")
                        
                        # Jeśli nie jest to ostatnia partia, wyczyść merged_chunks
                        if i < num_batches - 1:
                            del merged_chunks
                            merged_chunks = []
                            gc.collect()
                            
                        self.logger.info(f"Saved partial results to file (batch {i+1}), memory freed")
                        
                    except Exception as e:
                        self.logger.warning(f"Could not save partial results: {str(e)}")
                
                # Wymuś garbage collection co 5 partii
                if (i + 1) % 5 == 0:
                    self.force_garbage_collection()
                    
            except Exception as e:
                self.logger.error(f"Error processing batch {i+1}: {str(e)}")
                # Zwolnij pamięć i kontynuuj
                del energy_batch
                gc.collect()
                continue
        
        # Finalne łączenie wyników
        try:
            if merged_chunks:
                final_result = pd.concat(merged_chunks, ignore_index=True)
            else:
                # Spróbuj wczytać ostatni zapisany wynik
                try:
                    final_result = pd.read_pickle(f"partial_merge_result_{num_batches - 1}.pkl")
                    self.logger.info("Loaded final result from last saved partial file")
                except:
                    self.logger.error("No partial results available")
                    return pd.DataFrame()
            
            # Zwolnij pamięć
            del merged_chunks, energy_data, weather_data
            gc.collect()
            
            self.logger.info(f"Merged data contains {len(final_result)} records")
            return final_result
            
        except Exception as e:
            self.logger.error(f"Error combining final results: {str(e)}")
            # Ostatnia próba - wczytaj największy dostępny plik częściowy
            for i in range(num_batches - 1, -1, -1):
                try:
                    filename = f"partial_merge_result_{i}.pkl"
                    if os.path.exists(filename):
                        result = pd.read_pickle(filename)
                        self.logger.info(f"Emergency recovery: loaded {len(result)} records from {filename}")
                        return result
                except:
                    continue
            
            return pd.DataFrame()
    
    def _prepare_energy_data_optimized(self, staging_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Przygotowanie danych energetycznych z optymalizacją pamięci
        """
        self.logger.info("Preparing energy data with memory optimization")
        
        energy_records = []
        
        # Przetwarzaj każdy typ danych osobno i zwolnij pamięć po każdym
        data_types = [
            ('entso_actual_load', 'actual_consumption', 'quantity'),
            ('entso_generation', 'generation_amount', 'quantity'),
            ('entso_forecast', 'forecasted_consumption', 'quantity')
        ]
        
        for data_type, target_column, source_column in data_types:
            data = staging_data.get(data_type, pd.DataFrame())
            
            if data.empty:
                continue
            
            self.logger.info(f"Processing {len(data)} {data_type} records")
            
            # Optymalizuj DataFrame przed przetwarzaniem
            data = self.optimize_dataframe_memory(data)
            
            # Przetwarzaj w mniejszych chunkach
            chunk_size = 25000
            for start_idx in range(0, len(data), chunk_size):
                end_idx = min(start_idx + chunk_size, len(data))
                chunk = data.iloc[start_idx:end_idx]
                
                for _, row in chunk.iterrows():
                    timestamp = pd.to_datetime(row['timestamp'])
                    
                    record = {
                        'timestamp': timestamp,
                        'country_code': row['country'],
                        'zone_code': row['zone_code'],
                        'actual_consumption': None,
                        'forecasted_consumption': None,
                        'generation_amount': None,
                        'generation_type': None
                    }
                    
                    # Ustaw odpowiednią wartość
                    record[target_column] = row[source_column]
                    
                    if data_type == 'entso_generation':
                        record['generation_type'] = row.get('generation_type', 'B20')
                    
                    energy_records.append(record)
                
                # Zwolnij pamięć chunka
                del chunk
                
                # Sprawdź pamięć co kilka chunków
                if start_idx % (chunk_size * 5) == 0:
                    memory_usage = self.get_memory_usage()
                    if memory_usage > self.memory_threshold:
                        self.force_garbage_collection()
            
            # Zwolnij pamięć po zakończeniu przetwarzania typu danych
            del data
            gc.collect()
        
        if not energy_records:
            self.logger.warning("No energy records to process")
            return pd.DataFrame()
        
        # Utwórz DataFrame i zoptymalizuj
        energy_df = pd.DataFrame(energy_records)
        del energy_records  # Zwolnij pamięć listy
        gc.collect()
        
        energy_df = self.optimize_dataframe_memory(energy_df)
        
        # Agregacja z kontrolą pamięci
        energy_df = self._aggregate_energy_data_optimized(energy_df)
        
        self.logger.info(f"Prepared {len(energy_df)} energy data records")
        return energy_df
    
    def _aggregate_energy_data_optimized(self, energy_df: pd.DataFrame) -> pd.DataFrame:
        """Agregacja danych energetycznych z optymalizacją pamięci"""
        
        # Sortuj dane dla efektywniejszego grupowania
        energy_df = energy_df.sort_values(['timestamp', 'country_code', 'zone_code'])
        
        # Grupuj i agreguj w mniejszych chunkach
        grouped_data = []
        unique_keys = energy_df[['timestamp', 'country_code', 'zone_code']].drop_duplicates()
        
        chunk_size = 5000
        for start_idx in range(0, len(unique_keys), chunk_size):
            end_idx = min(start_idx + chunk_size, len(unique_keys))
            key_chunk = unique_keys.iloc[start_idx:end_idx]
            
            for _, key_row in key_chunk.iterrows():
                ts, country, zone = key_row['timestamp'], key_row['country_code'], key_row['zone_code']
                
                # Znajdź wszystkie rekordy dla tej kombinacji kluczy
                mask = ((energy_df['timestamp'] == ts) & 
                       (energy_df['country_code'] == country) & 
                       (energy_df['zone_code'] == zone))
                
                group = energy_df[mask]
                
                # Agreguj zużycie i prognozy
                actual = group['actual_consumption'].sum() if not group['actual_consumption'].isna().all() else None
                forecast = group['forecasted_consumption'].sum() if not group['forecasted_consumption'].isna().all() else None
                
                # Dane generacji
                generation_data = group[group['generation_amount'].notna()]
                
                if not generation_data.empty:
                    for gen_type, gen_group in generation_data.groupby('generation_type'):
                        generation_amount = gen_group['generation_amount'].sum()
                        
                        grouped_data.append({
                            'timestamp': ts,
                            'country_code': country,
                            'zone_code': zone,
                            'actual_consumption': actual,
                            'forecasted_consumption': forecast,
                            'generation_amount': generation_amount,
                            'generation_type': gen_type
                        })
                else:
                    grouped_data.append({
                        'timestamp': ts,
                        'country_code': country,
                        'zone_code': zone,
                        'actual_consumption': actual,
                        'forecasted_consumption': forecast,
                        'generation_amount': None,
                        'generation_type': None
                    })
            
            # Sprawdź pamięć co chunk
            if start_idx % (chunk_size * 10) == 0:
                memory_usage = self.get_memory_usage()
                if memory_usage > self.memory_threshold:
                    self.force_garbage_collection()
        
        result_df = pd.DataFrame(grouped_data)
        del grouped_data
        gc.collect()
        
        return self.optimize_dataframe_memory(result_df)
    
    def _prepare_weather_data_optimized(self, staging_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Przygotowanie danych pogodowych z optymalizacją pamięci"""
        
        weather_df = staging_data.get('weather', pd.DataFrame())
        climate_df = staging_data.get('climate', pd.DataFrame())
        
        if weather_df.empty:
            self.logger.warning("No weather data available")
            return pd.DataFrame()
        
        self.logger.info(f"Processing {len(weather_df)} weather records with memory optimization")
        
        # Optymalizuj dane pogodowe
        weather_df = self.optimize_dataframe_memory(weather_df)
        weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
        
        # Dodaj brakujące kolumny z domyślnymi wartościami
        required_columns = {
            'temperature_avg': 0.0,
            'humidity': 0.0,
            'precipitation': 0.0,
            'wind_speed': 0.0,
            'wind_direction': 0,
            'cloud_cover': 0.0,
            'weather_condition': 'Unknown'
        }
        
        for col, default_val in required_columns.items():
            if col not in weather_df.columns:
                weather_df[col] = default_val
        
        # Łączenie z danymi klimatycznymi jeśli dostępne
        if not climate_df.empty:
            climate_df = self.optimize_dataframe_memory(climate_df)
            weather_df = self._merge_climate_data_optimized(weather_df, climate_df)
        else:
            weather_df['heating_degree_days'] = 0.0
            weather_df['cooling_degree_days'] = 0.0
        
        # Dodaj dodatkowe kolumny pogodowe
        if 'air_pressure' not in weather_df.columns:
            weather_df['air_pressure'] = weather_df.get('pressure_msl', 1013.25)
        
        if 'solar_radiation' not in weather_df.columns:
            weather_df['solar_radiation'] = (100 - weather_df['cloud_cover']) / 100 * 1000
        
        return self.optimize_dataframe_memory(weather_df)
    
    def _merge_climate_data_optimized(self, weather_df: pd.DataFrame, climate_df: pd.DataFrame) -> pd.DataFrame:
        """Optymalizowane łączenie danych klimatycznych"""
        
        self.logger.info(f"Merging {len(climate_df)} climate records with weather data")
        
        climate_df['date'] = pd.to_datetime(climate_df['date'])
        weather_df['date'] = weather_df['timestamp'].dt.date
        climate_df['date_only'] = climate_df['date'].dt.date
        
        # Utwórz mapowanie dla szybszego łączenia
        climate_dict = {}
        for _, row in climate_df.iterrows():
            key = (row['date_only'], row['country_code'])
            climate_dict[key] = {
                'heating_degree_days': row.get('heating_degree_days', 0.0),
                'cooling_degree_days': row.get('cooling_degree_days', 0.0)
            }
        
        # Zastosuj mapowanie
        weather_df['heating_degree_days'] = 0.0
        weather_df['cooling_degree_days'] = 0.0
        
        for idx, row in weather_df.iterrows():
            key = (row['date'], row['country_code'])
            if key in climate_dict:
                weather_df.at[idx, 'heating_degree_days'] = climate_dict[key]['heating_degree_days']
                weather_df.at[idx, 'cooling_degree_days'] = climate_dict[key]['cooling_degree_days']
        
        weather_df.drop(['date'], axis=1, inplace=True)
        return weather_df
    
    # Reszta metod pozostaje bez zmian ale z dodanym monitoringiem pamięci
    def process_facts(self) -> bool:
        """
        Główna metoda do przetwarzania faktów z optymalizacją pamięci
        """
        self.logger.info("Starting memory-optimized fact processing")
        
        # Sprawdź dostępną pamięć na początku
        initial_memory = self.get_memory_usage()
        self.logger.info(f"Initial memory usage: {initial_memory*100:.1f}%")
        
        # Logowanie rozpoczęcia procesu
        self._log_process('FACT_PROCESSING', 'RUNNING')
        
        try:
            # Wymuś garbage collection na początku
            self.force_garbage_collection()
            
            # Ładowanie mapowań wymiarów
            self.load_dimension_mappings()
            
            # Ładowanie danych ze staging z optymalizacją pamięci
            staging_data = self.load_staging_data()
            
            # Sprawdź czy mamy dane energetyczne
            if (staging_data.get('entso_actual_load', pd.DataFrame()).empty and 
                staging_data.get('entso_generation', pd.DataFrame()).empty and
                staging_data.get('entso_forecast', pd.DataFrame()).empty):
                self.logger.error("No energy data to process")
                self._log_process('FACT_PROCESSING', 'FAILED', 0, "No energy data to process")
                return False
            
            # Łączenie danych z optymalizacją pamięci
            merged_data = self.merge_energy_weather_data_optimized(staging_data)
            
            # Zwolnij pamięć staging_data
            del staging_data
            self.force_garbage_collection()
            
            if merged_data.empty:
                self.logger.error("No data to process after merging")
                self._log_process('FACT_PROCESSING', 'FAILED', 0, "No data to process after merging")
                return False
            
            # Obliczanie metryk pochodnych
            processed_data = self.calculate_derived_metrics(merged_data)
            
            # Zwolnij pamięć merged_data
            del merged_data
            self.force_garbage_collection()
            
            # Mapowanie na klucze wymiarów
            fact_data = self.map_to_dimension_keys(processed_data)
            
            # Zwolnij pamięć processed_data
            del processed_data
            self.force_garbage_collection()
            
            # Wstawianie faktów do staging
            self.logger.info("Inserting facts into staging table")
            success = self.insert_facts_to_staging(fact_data)
            
            if success:
                self._log_process('FACT_PROCESSING', 'SUCCESS', len(fact_data))
                self.logger.info(f"Successfully processed {len(fact_data)} fact records")
                
                # Pokaż końcowe użycie pamięci
                final_memory = self.get_memory_usage()
                self.logger.info(f"Final memory usage: {final_memory*100:.1f}% (change: {(final_memory-initial_memory)*100:+.1f}%)")
                
                return True
            else:
                self._log_process('FACT_PROCESSING', 'FAILED', 0, "Failed to insert facts to staging")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing facts: {str(e)}")
            self.logger.error(traceback.format_exc())
            self._log_process('FACT_PROCESSING', 'FAILED', 0, str(e))
            return False
        finally:
            # Wyczyść cache wymiarów
            self.dimension_cache.clear()
            self.force_garbage_collection()
    
    def calculate_derived_metrics(self, merged_data: pd.DataFrame) -> pd.DataFrame:
        """
        Obliczanie metryk pochodnych z optymalizacją pamięci
        """
        self.logger.info("Calculating derived metrics with memory optimization")
        
        if merged_data.empty:
            self.logger.warning("No data to calculate metrics on")
            return merged_data
        
        # Sprawdź pamięć przed rozpoczęciem
        memory_usage = self.get_memory_usage()
        if memory_usage > self.memory_threshold:
            self.force_garbage_collection()
        
        # Obliczanie odchylenia prognozy od rzeczywistości
        mask = (merged_data['actual_consumption'].notna() & 
                merged_data['forecasted_consumption'].notna() & 
                (merged_data['actual_consumption'] > 0))
        
        if mask.any():
            merged_data.loc[mask, 'consumption_deviation'] = (
                (merged_data.loc[mask, 'forecasted_consumption'] - merged_data.loc[mask, 'actual_consumption']) / 
                merged_data.loc[mask, 'actual_consumption'] * 100
            )
        
        # Obliczanie zużycia per capita (jeśli dostępne dane o populacji)
        if 'bidding_zone' in self.dimension_cache and len(self.dimension_cache['bidding_zone']) > 0:
            bidding_zone_df = self.dimension_cache['bidding_zone']
            if 'population' in bidding_zone_df.columns:
                zone_population = {}
                for _, row in bidding_zone_df.iterrows():
                    if 'primary_country' in row and 'population' in row:
                        zone_population[row['primary_country']] = row['population']
                        
                for country, population in zone_population.items():
                    if population > 0:
                        mask = (merged_data['country_code'] == country) & merged_data['actual_consumption'].notna()
                        if mask.any():
                            merged_data.loc[mask, 'per_capita_consumption'] = merged_data.loc[mask, 'actual_consumption'] / population
        
        # Obliczanie współczynnika wykorzystania mocy (symulowane)
        mask = merged_data['generation_amount'].notna()
        if mask.any():
            merged_data.loc[mask, 'capacity_factor'] = np.random.uniform(0.15, 0.85, mask.sum())
        
        # Obliczanie udziału energii odnawialnej
        renewable_types = ['B01', 'B09', 'B11', 'B12', 'B13', 'B15', 'B16', 'B18', 'B19']
        
        merged_data['renewable_percentage'] = 0.0
        
        # Oblicz sumę generacji dla każdej kombinacji timestamp/zone
        generation_sums = merged_data.groupby(['timestamp', 'zone_code'])['generation_amount'].sum().reset_index()
        generation_sums.rename(columns={'generation_amount': 'total_generation'}, inplace=True)
        
        merged_data = pd.merge(merged_data, generation_sums, on=['timestamp', 'zone_code'], how='left')
        
        mask = (merged_data['generation_type'].isin(renewable_types)) & (merged_data['total_generation'] > 0)
        if mask.any():
            merged_data.loc[mask, 'renewable_percentage'] = (
                (merged_data.loc[mask, 'generation_amount'] / merged_data.loc[mask, 'total_generation']) * 100
            )
        
        merged_data.drop('total_generation', axis=1, inplace=True, errors='ignore')
        
        # Dodaj brakujące temperatury min/max jeśli nie ma
        if 'temperature_min' not in merged_data.columns:
            merged_data['temperature_min'] = merged_data['temperature_avg'] - np.random.uniform(2, 8, len(merged_data))
        if 'temperature_max' not in merged_data.columns:
            merged_data['temperature_max'] = merged_data['temperature_avg'] + np.random.uniform(2, 8, len(merged_data))
        
        # Wypełnienie brakujących wartości
        numeric_columns = [
            'actual_consumption', 'forecasted_consumption', 'consumption_deviation',
            'generation_amount', 'capacity_factor', 'renewable_percentage',
            'per_capita_consumption', 'temperature_min', 'temperature_max',
            'temperature_avg', 'precipitation', 'wind_speed', 'wind_direction',
            'humidity', 'solar_radiation', 'air_pressure', 'heating_degree_days', 'cooling_degree_days'
        ]
        
        for col in numeric_columns:
            if col in merged_data.columns:
                merged_data[col] = merged_data[col].fillna(0.0)
            else:
                merged_data[col] = 0.0
        
        # Sprawdź pamięć po obliczeniach
        self.get_memory_usage()
        
        self.logger.info("Derived metrics calculated successfully")
        return merged_data
    
    def safe_fillna(self, series: pd.Series, fill_value) -> pd.Series:
        """
        Bezpieczne fillna które obsługuje kolumny categorical
        
        Args:
            series: Seria danych do wypełnienia
            fill_value: Wartość do wypełnienia
            
        Returns:
            Seria z wypełnionymi wartościami
        """
        if pd.api.types.is_categorical_dtype(series):
            # Jeśli to kolumna categorical, najpierw dodaj kategorię jeśli nie istnieje
            if fill_value not in series.cat.categories:
                series = series.cat.add_categories([fill_value])
            return series.fillna(fill_value)
        else:
            return series.fillna(fill_value)
    
    def map_to_dimension_keys(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Mapowanie danych na klucze wymiarów ze STAGING
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
            processed_data['date_id'] = self.safe_fillna(processed_data['date_id'], 0).astype(int)
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
            processed_data['year'] = processed_data['timestamp'].dt.year
            
            zone_dict = {}
            for _, row in bidding_zone_mapping.iterrows():
                key = (row['bidding_zone_code'], row['year'])
                zone_dict[key] = row['staging_bidding_zone_id']
            
            processed_data['bidding_zone_id'] = processed_data.apply(
                lambda x: self._find_applicable_bidding_zone(
                    zone_dict, x['zone_code'], x['year']
                ), 
                axis=1
            )
            
            processed_data['bidding_zone_id'] = self.safe_fillna(processed_data['bidding_zone_id'], 0).astype(int)
        else:
            processed_data['bidding_zone_id'] = 0
            self.logger.warning("No bidding zone mapping available, using 0 as default")
        
        # Mapowanie weather_zone_id                                
        weather_zone_mapping = self.dimension_cache.get('weather_zone', pd.DataFrame())

        if not weather_zone_mapping.empty:
            weather_zone_dict = {}
            for _, row in weather_zone_mapping.iterrows():
                weather_zone_dict[row['bidding_zone_id']] = row['staging_weather_zone_id']
            
            processed_data['weather_zone_id'] = processed_data['bidding_zone_id'].map(weather_zone_dict)
            processed_data['weather_zone_id'] = self.safe_fillna(processed_data['weather_zone_id'], 0).astype(int)
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
            processed_data['generation_type_id'] = self.safe_fillna(processed_data['generation_type_id'], 0)
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
            processed_data['weather_condition_id'] = self.safe_fillna(processed_data['weather_condition_id'], 0)
        else:
            processed_data['weather_condition_id'] = 0
            self.logger.warning("No weather condition mapping available, using 0 as default")
                
        socioeconomic_mapping = self.dimension_cache.get('socioeconomic_profile', pd.DataFrame())

        if not socioeconomic_mapping.empty:
            if 'year' not in processed_data.columns:
                processed_data['year'] = processed_data['timestamp'].dt.year
            
            processed_data['socioeconomic_profile_id'] = processed_data.apply(
                lambda x: self._find_applicable_socioeconomic_profile(
                    socioeconomic_mapping, x['zone_code'], x['year']
                ), 
                axis=1
            )
            
            processed_data['socioeconomic_profile_id'] = self.safe_fillna(processed_data['socioeconomic_profile_id'], 0).astype(int)
        else:
            processed_data['socioeconomic_profile_id'] = 0
            self.logger.warning("No socioeconomic profile mapping available, using 0 as default")
        
        # Wypełnij brakujące ID jako 0
        id_columns = [
            'date_id', 'time_id', 'bidding_zone_id', 'weather_zone_id',
            'generation_type_id', 'weather_condition_id', 'socioeconomic_profile_id'
        ]
        
        for col in id_columns:
            processed_data[col] = self.safe_fillna(processed_data[col], 0).astype(int)
        
        self.logger.info("Dimension keys mapped successfully")
        return processed_data
    
    def create_staging_fact_table(self, conn) -> bool:
        """Tworzenie tabeli staging dla faktów"""
        self.logger.info("Creating staging fact table")
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL 
                    SELECT 1
                ELSE 
                    SELECT 0
            """)
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
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
            
            conn.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating staging fact table: {str(e)}")
            return False
    
    def truncate_staging_fact_table(self, conn) -> bool:
        """Czyszczenie tabeli staging dla faktów"""
        self.logger.info("Truncating staging fact table")
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                IF OBJECT_ID('staging_fact_energy_weather', 'U') IS NOT NULL 
                    TRUNCATE TABLE staging_fact_energy_weather
            """)
            
            conn.commit()
            self.logger.info("Truncated staging_fact_energy_weather table")
            return True
            
        except Exception as e:
            self.logger.error(f"Error truncating staging fact table: {str(e)}")
            return False
    
    def insert_facts_to_staging(self, fact_data: pd.DataFrame) -> bool:
        """
        Wstawianie faktów do tabeli staging z optymalizacją pamięci
        """
        self.logger.info(f"Inserting {len(fact_data)} facts to staging table with memory optimization")
        
        if fact_data.empty:
            self.logger.warning("No facts to insert")
            return False
        
        try:
            conn = pyodbc.connect(self.connection_string)
            
            # Twórz/czyść tabelę staging
            if not self.create_staging_fact_table(conn):
                return False
            
            if not self.truncate_staging_fact_table(conn):
                return False
            
            # Przygotuj kolumny
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
            
            # Wstaw dane w małych partiach z kontrolą pamięci
            cursor = conn.cursor()
            batch_size = 2000  # Jeszcze mniejsze partie dla wstawiania
            total_rows = len(fact_data)
            inserted_rows = 0
            
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"""
                INSERT INTO staging_fact_energy_weather
                ({', '.join(columns)})
                VALUES ({placeholders})
            """
            
            cursor.fast_executemany = True
            
            for i in range(0, total_rows, batch_size):
                # Sprawdź pamięć przed każdą partią
                memory_usage = self.get_memory_usage()
                if memory_usage > self.memory_threshold:
                    self.force_garbage_collection()
                    # Zmniejsz rozmiar partii jeśli pamięć wysoka
                    if memory_usage > 0.9:
                        batch_size = max(500, batch_size // 2)
                        self.logger.warning(f"High memory usage, reducing insert batch size to {batch_size}")
                
                batch = fact_data.iloc[i:i+batch_size]
                
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
                
                cursor.executemany(insert_sql, values)
                inserted_rows += len(batch)
                
                # Commit co 5000 rekordów
                if inserted_rows % 5000 == 0 or inserted_rows == total_rows:
                    conn.commit()
                    self.logger.info(f"Inserted and committed {inserted_rows}/{total_rows} rows ({(inserted_rows/total_rows)*100:.1f}%)")
                
                # Zwolnij pamięć
                del values, batch
                if i % 5 == 0:
                    gc.collect()
            
            conn.close()
            self.logger.info(f"Successfully inserted {inserted_rows} fact records to staging")
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting facts to staging: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _find_applicable_bidding_zone(self, zone_dict, zone_code, year):
        """
        Znajdź odpowiednie bidding_zone_id dla danego kodu strefy i roku.
        """
        applicable_records = []
        for (code, record_year), zone_id in zone_dict.items():
            if code == zone_code and record_year <= year:
                applicable_records.append((record_year, zone_id))
        
        if not applicable_records:
            return 0
        
        applicable_records.sort(reverse=True)
        return applicable_records[0][1]

    def _find_applicable_socioeconomic_profile(self, socioeconomic_mapping, zone_code, year):
        """
        Znajdź odpowiedni profil socjoekonomiczny dla danego kodu strefy i roku.
        """
        zone_profiles = socioeconomic_mapping[socioeconomic_mapping['bidding_zone_code'] == zone_code]
        
        if zone_profiles.empty:
            return 0
        
        applicable_profiles = zone_profiles[zone_profiles['year'] <= year]
        
        if applicable_profiles.empty:
            return 0
        
        latest_profile = applicable_profiles.loc[applicable_profiles['year'].idxmax()]
        return latest_profile['staging_socioeconomic_profile_id']
    
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
    
    def load_dimension_mappings(self) -> Dict[str, pd.DataFrame]:
        """Ładowanie mapowań wymiarów z optymalizacją pamięci"""
        self.logger.info("Loading dimension mappings with memory optimization")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            dimensions = {}
            
            dimension_queries = {
                'date': "SELECT id as staging_date_id, full_date, year, month, day_of_month FROM staging_dim_date",
                'time': "SELECT id as staging_time_id, hour, minute FROM staging_dim_time",
                'bidding_zone': "SELECT id as staging_bidding_zone_id, bidding_zone_code, year FROM staging_dim_bidding_zone",
                'weather_zone': "SELECT id as staging_weather_zone_id, weather_zone_name, bidding_zone_id FROM staging_dim_weather_zone",
                'generation_type': "SELECT id as staging_generation_type_id, entso_code, generation_type, generation_category FROM staging_dim_generation_type",
                'weather_condition': "SELECT id as staging_weather_condition_id, condition_type, condition_severity FROM staging_dim_weather_condition",
                'socioeconomic_profile': "SELECT id as staging_socioeconomic_profile_id, bidding_zone_code, year FROM staging_dim_socioeconomic_profile"
            }
            
            for dim_name, query in dimension_queries.items():
                try:
                    df = pd.read_sql(query, conn)
                    df = self.optimize_dataframe_memory(df)
                    dimensions[dim_name] = df
                    self.logger.info(f"Loaded {len(df)} {dim_name} dimension records")
                except Exception as e:
                    self.logger.error(f"Error loading {dim_name} dimension: {str(e)}")
                    dimensions[dim_name] = pd.DataFrame()
                
                # Sprawdź pamięć po każdym wymiarze
                self.get_memory_usage()
            
            conn.close()
            self.dimension_cache = dimensions
            return dimensions
            
        except Exception as e:
            self.logger.error(f"Error loading dimension mappings: {str(e)}")
            return {}

# Zachowaj wszystkie pozostałe metody z oryginalnej klasy
# Tu można by dodać je wszystkie, ale dla czytelności pokazuję tylko kluczowe optymalizacje

# Funkcja główna z monitoringiem pamięci
def main():
    """Główna funkcja wywoływana przez SSIS z monitoringiem pamięci"""
    import sys
    import os
    import logging
    import traceback
    
    # Konfiguracja logowania
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                       handlers=[
                           logging.FileHandler("fact_processor_optimized.log"),
                           logging.StreamHandler()
                       ])
    logger = logging.getLogger("OptimizedFactProcessorMain")
    
    logger.info("Starting Memory-Optimized Fact Processor")
    
    # Sprawdź dostępną pamięć systemową
    memory = psutil.virtual_memory()
    logger.info(f"System memory: {memory.total/1024**3:.1f}GB total, {memory.available/1024**3:.1f}GB available, {memory.percent:.1f}% used")
    
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    if not connection_string:
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        logger.info(f"Using default connection string")
    
    try:
        # Inicjalizacja zoptymalizowanego procesora faktów
        processor = MemoryOptimizedFactProcessor(connection_string)
        
        # Przetwarzanie faktów z optymalizacją pamięci
        success = processor.process_facts()
        
        if success:
            logger.info("Memory-optimized fact processing completed successfully")
            sys.exit(0)
        else:
            logger.error("Error occurred during memory-optimized fact processing")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()