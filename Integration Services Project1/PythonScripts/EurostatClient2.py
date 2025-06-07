"""
EurostatClient.py
Klient API do komunikacji z Eurostat Database
Pobiera dane socjoekonomiczne dla krajów UE
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Tuple, Any, Union
import pyodbc
import json
import os
import sys
import traceback

class EurostatClient:
    """Klient API dla Eurostat Database - POPRAWIONA WERSJA"""
    
    def __init__(self, connection_string: str = None):
        """
        Inicjalizacja klienta Eurostat
        
        Args:
            connection_string: String połączenia z bazą danych
        """
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
        self.connection_string = connection_string
        
        # Domyślne parametry dla każdego zapytania
        self.default_params = {
            'format': 'JSON',
            'lang': 'EN'
        }
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Mapowanie krajów na kody ISO - ROZSZERZONO LISTĘ
        self.country_mapping = {
            'IT': 'Italy',
            'DE': 'Germany',
            'HU': 'Hungary',
            'PL': 'Poland',
            'SK': 'Slovakia',
            'FR': 'France',
            'ES': 'Spain',
            'CZ': 'Czech Republic',
            'AT': 'Austria',
            'NL': 'Netherlands',
            'BE': 'Belgium',
            'PT': 'Portugal',
            'GR': 'Greece',
            'RO': 'Romania',
            'BG': 'Bulgaria',
            'HR': 'Croatia',
            'SI': 'Slovenia',
            'LT': 'Lithuania',
            'LV': 'Latvia',
            'EE': 'Estonia',
            'FI': 'Finland',
            'SE': 'Sweden',
            'DK': 'Denmark',
            'IE': 'Ireland',
            'LU': 'Luxembourg',
            'MT': 'Malta',
            'CY': 'Cyprus'
        }
        
        # Priorytetowe kody krajów - WSZYSTKIE KRAJE Z ENTSO-E
        self.priority_countries = ['PL', 'DE', 'FR', 'ES', 'IT', 'CZ', 'SK', 'HU', 'AT', 'SI', 'HR', 'RO', 'BG', 'GR', 'BE', 'NL', 'LU', 'IE', 'DK', 'SE', 'FI', 'EE', 'LV', 'LT', 'PT']
        
        # KONFIGURACJA DATASETÓW - ZAKTUALIZOWANA WERSJA NA PODSTAWIE SZCZEGÓŁOWYCH TESTÓW
        # Updated dataset configurations for EurostatClient
        # This represents the section to replace in your original code

        # KONFIGURACJA DATASETÓW - ZAKTUALIZOWANA WERSJA NA PODSTAWIE DOKŁADNYCH BADAŃ API
        self.datasets = {
            'population': {
                'code': 'demo_pjan',
                'description': 'Population by sex and age',
                'params': {
                    'sex': 'T',      # Total
                    'age': 'TOTAL'   # All ages
                }
            },
            'gdp_per_capita': {
                'code': 'nama_10_pc',
                'description': 'GDP per capita',
                'params': {
                    'unit': 'CP_EUR_HAB',  # Current prices, euro per capita
                    'na_item': 'B1GQ'      # Gross domestic product at market prices
                },
                'alternative_units': ['CP_PPS_EU27_2020_HAB']  # Alternatywna jednostka PPS
            },
            'electricity_prices': {
                'code': 'nrg_pc_204',
                'description': 'Electricity prices for household consumers',
                'params': {
                    'unit': 'KWH',         # Per kWh
                    'product': '6000',     # Electricity
                    'nrg_cons': 'KWH2500-4999',  # Band DC
                    'tax': 'X_TAX',        # Excluding taxes
                    'currency': 'EUR'
                }
            },
            'energy_intensity': {
                'code': 'sdg_07_30',  # Zmiana kodu z nrg_ind_ei na sdg_07_30
                'description': 'Energy intensity of the economy',
                'params': {
                    'unit': 'EUR_KGOE'  # Dodanie parametru jednostki
                },
                'alternative_params': [
                    {
                        # Bez parametrów jako alternatywa
                    }
                ]
            },
            'unemployment_rate': {
                'code': 'une_rt_a',
                'description': 'Unemployment rate by sex and age - annual data',
                'params': {
                    'sex': 'T',      # Total
                    'age': 'Y15-74', # Age group 15-74
                    'unit': 'PC_ACT' # Percentage of active population
                },
                'alternative_units': ['THS_PER'],
                'alternative_age_params': ['Y_GE15', 'Y15-64']  # Fallback age parameters
            },
            # ZAKTUALIZOWANO: Zmieniono nazwę i kod datasetu zgodnie z wymogami
            'poverty_by_degree_of_urbanization': {
                'code': 'ilc_sbjp05',
                'description': 'Subjective poverty by degree of urbanization',
                'params': {
                    # Bez parametrów - najlepsze wyniki (90 niepustych wartości)
                },
                'alternative_params': [
                    {
                        'unit': 'PC'  # Również daje 90 niepustych wartości
                    }
                ]
            },
            'service_sector': {
                'code': 'sts_sepr_a',  # ZMIENIONO: parametr indic_bt nie jest potrzebny
                'description': 'Production in services - annual data',
                'params': {
                    's_adj': 'CA',       # Calendar adjusted data - tylko ten parametr działa
                    'nace_r2': 'G-N_X_K'  # Services excluding financial services
                }
            },
            
            'industry_sector': {
                'code': 'sts_inpr_a',  # ZMIENIONO: parametr indic_bt nie jest potrzebny
                'description': 'Production in industry - annual data',
                'params': {
                    's_adj': 'CA',     # Calendar adjusted data - tylko ten parametr działa
                    'nace_r2': 'B-D'  # Industry excluding construction
                }
            },
            
            'household_size': {
                'code': 'lfst_hhantych',  # ZMIENIONO: parametr hhtyp nie jest obsługiwany
                'description': 'Average number of persons per household by household composition',
                'params': {
                    # Bez parametrów - testy pokazały że działa tylko bez parametrów
                }
            },
            'energy_poverty': {
                'code': 'sdg_07_60',  # Ten działa poprawnie
                'description': 'Population unable to keep home adequately warm by poverty status',
                'params': {
                    'unit': 'PC',     # Percentage
                    'incgrp': 'TOTAL'  # Total population
                }
            },
            'heating_systems': {
                'code': 'nrg_pc_202',  # Dataset z najlepszymi wynikami (2664 rekordów)
                'description': 'Primary heating systems and gas prices',
                'params': {
                    # Bez parametrów - najlepsze wyniki
                },
                'fallback_dataset': [
                    {
                        'code': 'nrg_d_hhq',
                        'params': {
                            # Bez parametrów - 2220 rekordów
                        }
                    },
                    {
                        'code': 'nrg_d_hhq',
                        'params': {
                            'nrg_bal': 'FC_OTH_HH_E'  # 420 rekordów
                        }
                    },
                    {
                        'code': 'nrg_bal_c',
                        'params': {
                            'unit': 'TJ',
                            'siec': 'TOTAL',
                            'nrg_bal': 'FC_OTH_HH_E'  # 20 rekordów
                        }
                    }
                ]
            }
        }
    
    def _make_request(self, dataset_code: str, params: Dict, max_retries: int = 3) -> Optional[Dict]:
        """
        Wykonanie zapytania do API Eurostat
        
        Args:
            dataset_code: Kod datasetu Eurostat
            params: Parametry zapytania
            max_retries: Maksymalna liczba prób
            
        Returns:
            Odpowiedź JSON jako dict lub None w przypadku błędu
        """
        url = f"{self.base_url}/{dataset_code}"
        
        # Dodaj domyślne parametry
        all_params = self.default_params.copy()
        all_params.update(params)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Sending request to: {url} with params: {all_params}")
                response = requests.get(url, params=all_params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    # Sprawdź, czy są dane w odpowiedzi
                    if 'value' in data and data['value']:
                        return data
                    else:
                        self.logger.warning(f"No data values in response for dataset {dataset_code}")
                        return data  # Zwracamy pustą odpowiedź, ale nie None
                elif response.status_code == 413:  # Asynchronous Response
                    self.logger.warning(f"Asynchronous response received, waiting...")
                    time.sleep(30)
                elif response.status_code == 429:  # Too Many Requests
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        return None
    
    def _make_request_with_fallback(self, dataset_info: Dict, countries: List[str] = None, 
                                  since_year: int = 2020, max_retries: int = 3) -> Optional[Dict]:
        """
        Wykonanie zapytania z fallback na alternatywne parametry i datasety
        
        Args:
            dataset_info: Informacje o datasecie
            countries: Lista krajów
            since_year: Rok początkowy
            max_retries: Maksymalna liczba prób
            
        Returns:
            Odpowiedź JSON jako dict lub None w przypadku błędu
        """
        dataset_code = dataset_info['code']
        base_params = dataset_info['params'].copy() if 'params' in dataset_info else {}
        base_params['sinceTimePeriod'] = str(since_year)
        
        if countries:
            base_params['geo'] = countries
        
        # 1. Próba z domyślnymi parametrami
        self.logger.info(f"Trying primary parameters for {dataset_code}")
        response = self._make_request(dataset_code, base_params, max_retries)
        
        if response and response.get('value'):
            return response
        
        # 2. Fallback na alternatywne jednostki (jeśli są)
        alternative_units = dataset_info.get('alternative_units', [])
        if alternative_units and 'unit' in base_params:
            self.logger.info(f"Trying alternative units for {dataset_code}: {alternative_units}")
            
            for alt_unit in alternative_units:
                alt_params = base_params.copy()
                alt_params['unit'] = alt_unit
                
                response = self._make_request(dataset_code, alt_params, max_retries)
                
                if response and response.get('value'):
                    self.logger.info(f"Success with alternative unit: {alt_unit}")
                    return response
        
        # 3. Fallback na alternatywne parametry (jeśli są)
        alternative_params_list = dataset_info.get('alternative_params', [])
        if alternative_params_list:
            self.logger.info(f"Trying alternative parameter sets for {dataset_code}")
            
            for alt_params_set in alternative_params_list:
                alt_params = alt_params_set.copy()
                alt_params['sinceTimePeriod'] = str(since_year)
                
                if countries:
                    alt_params['geo'] = countries
                
                response = self._make_request(dataset_code, alt_params, max_retries)
                
                if response and response.get('value'):
                    self.logger.info(f"Success with alternative parameters: {alt_params}")
                    return response
        
        # 4. Specjalny fallback dla bezrobocia - dodaj age parameter
        if dataset_code == 'une_rt_a' and 'alternative_age_params' in dataset_info:
            self.logger.info(f"Trying age parameters for unemployment dataset")
            
            for age_param in dataset_info['alternative_age_params']:
                alt_params = base_params.copy()
                alt_params['age'] = age_param
                
                response = self._make_request(dataset_code, alt_params, max_retries)
                
                if response and response.get('value'):
                    self.logger.info(f"Success with age parameter: {age_param}")
                    return response
        
        # 5. Fallback na alternatywny dataset (jeśli jest)
        fallback_dataset = dataset_info.get('fallback_dataset')
        if fallback_dataset:
            # Obsługa zarówno pojedynczego fallback datasetu jak i listy
            if isinstance(fallback_dataset, list):
                # Iteracja przez listę alternatywnych datasetów
                for fb_dataset in fallback_dataset:
                    self.logger.info(f"Trying fallback dataset: {fb_dataset['code']}")
                    
                    fallback_params = fb_dataset.get('params', {}).copy()
                    fallback_params['sinceTimePeriod'] = str(since_year)
                    
                    if countries:
                        fallback_params['geo'] = countries
                    
                    response = self._make_request(fb_dataset['code'], fallback_params, max_retries)
                    
                    if response and response.get('value'):
                        self.logger.info(f"Success with fallback dataset: {fb_dataset['code']}")
                        return response
            else:
                # Obsługa pojedynczego fallback datasetu (poprzednia implementacja)
                self.logger.info(f"Trying fallback dataset: {fallback_dataset['code']}")
                
                fallback_params = fallback_dataset.get('params', {}).copy()
                fallback_params['sinceTimePeriod'] = str(since_year)
                
                if countries:
                    fallback_params['geo'] = countries
                
                response = self._make_request(fallback_dataset['code'], fallback_params, max_retries)
                
                if response and response.get('value'):
                    self.logger.info(f"Success with fallback dataset: {fallback_dataset['code']}")
                    return response
        
        self.logger.warning(f"All attempts failed for {dataset_code}")
        return None
    
    def _parse_eurostat_response(self, response_data: Dict, dataset_name: str) -> pd.DataFrame:
        """Parsowanie odpowiedzi Eurostat do DataFrame"""
        
        try:
            if not response_data or 'value' not in response_data:
                self.logger.warning(f"No data values in response for {dataset_name}")
                return pd.DataFrame()
            
            values = response_data['value']
            dimension = response_data['dimension']
            
            # Sprawdź czy mamy dane
            if not values:
                self.logger.warning(f"Empty values in response for {dataset_name}")
                return self._create_empty_dataframe_with_structure(dataset_name)
            
            # Pobierz informacje o wymiarach
            geo_data = dimension.get('geo', {}).get('category', {})
            time_data = dimension.get('time', {}).get('category', {})
            
            geo_indices = geo_data.get('index', {})
            geo_labels = geo_data.get('label', {})
            time_indices = time_data.get('index', {})
            
            records = []
            
            # Parsowanie specyficzne dla każdego datasetu
            if dataset_name == 'population':
                records = self._parse_population_data(values, geo_indices, geo_labels, time_indices)
            elif dataset_name == 'electricity_prices':
                records = self._parse_electricity_data(values, geo_indices, geo_labels, time_indices)
            elif dataset_name == 'energy_intensity':
                # Specjalna obsługa dla energy_intensity
                records = self._parse_energy_intensity_data(values, geo_indices, geo_labels, time_indices, dimension)
            else:
                records = self._parse_generic_data(values, geo_indices, geo_labels, time_indices, dataset_name)
            
            # Dodaj dataset name do wszystkich rekordów
            for record in records:
                record['dataset'] = dataset_name
            
            df = pd.DataFrame(records)
            self.logger.info(f"Successfully parsed {len(df)} records for {dataset_name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing response for {dataset_name}: {str(e)}")
            traceback.print_exc()
            return self._create_empty_dataframe_with_structure(dataset_name)
    
    def _parse_population_data(self, values: Dict, geo_indices: Dict, geo_labels: Dict, time_indices: Dict) -> List[Dict]:
        """Parsowanie danych populacji"""
        records = []
        country_codes = list(geo_indices.keys())
        time_codes = list(time_indices.keys())
        
        for value_key, value in values.items():
            try:
                key_num = int(value_key)
                country_idx = key_num // len(time_codes)
                time_idx = key_num % len(time_codes)
                
                if country_idx < len(country_codes) and time_idx < len(time_codes):
                    country_code = country_codes[country_idx]
                    time_period = time_codes[time_idx]
                    
                    record = {
                        'country_code': country_code,
                        'country_name': geo_labels.get(country_code, country_code),
                        'time_period': time_period,
                        'value': float(value) if value is not None else None
                    }
                    records.append(record)
            except (ValueError, IndexError):
                continue
        
        return records
    
    def _parse_electricity_data(self, values: Dict, geo_indices: Dict, geo_labels: Dict, time_indices: Dict) -> List[Dict]:
        """Parsowanie danych cen energii"""
        records = []
        country_codes = list(geo_indices.keys())
        time_codes = list(time_indices.keys())
        
        for value_key, value in values.items():
            try:
                key_num = int(value_key)
                country_idx = key_num // len(time_codes)
                time_idx = key_num % len(time_codes)
                
                if country_idx < len(country_codes) and time_idx < len(time_codes):
                    country_code = country_codes[country_idx]
                    time_period = time_codes[time_idx]
                    
                    record = {
                        'country_code': country_code,
                        'country_name': geo_labels.get(country_code, country_code),
                        'time_period': time_period,
                        'value': float(value) if value is not None else None
                    }
                    records.append(record)
            except (ValueError, IndexError):
                continue
        
        return records
    def _parse_energy_intensity_data(self, values, geo_indices, geo_labels, time_indices, dimension):
        """Parsowanie danych intensywności energetycznej"""
        records = []
        
        country_codes = list(geo_indices.keys())
        time_codes = list(time_indices.keys())
        
        # Sprawdź format kluczy
        sample_key = next(iter(values.keys()), None)
        is_numeric = False
        
        try:
            if sample_key:
                int(sample_key)  # Sprawdź, czy klucz to liczba
                is_numeric = True
        except ValueError:
            is_numeric = False
        
        if is_numeric:
            # Metoda numeryczna dla kluczy liczbowych
            for value_key, value in values.items():
                try:
                    key_num = int(value_key)
                    
                    if len(time_codes) > 0:
                        country_idx = key_num // len(time_codes)
                        time_idx = key_num % len(time_codes)
                        
                        if country_idx < len(country_codes) and time_idx < len(time_codes):
                            country_code = country_codes[country_idx]
                            time_period = time_codes[time_idx]
                            
                            record = {
                                'country_code': country_code,
                                'country_name': geo_labels.get(country_code, country_code),
                                'time_period': time_period,
                                'value': float(value) if value is not None else None
                            }
                            records.append(record)
                except (ValueError, IndexError, ZeroDivisionError):
                    continue
        elif ":" in sample_key:
            # Metoda dla kluczy z separatorami
            dim_positions = {}
            key_parts = sample_key.split(':')
            
            # Określ, które pozycje odpowiadają którym wymiarom
            for i, part in enumerate(key_parts):
                for dim_name, dim_data in dimension.items():
                    if 'category' in dim_data and 'index' in dim_data['category']:
                        if part in dim_data['category']['index']:
                            dim_positions[dim_name] = i
                            break
            
            for key, value in values.items():
                if value is None:
                    continue
                    
                key_parts = key.split(':')
                
                country_code = None
                time_period = None
                
                if 'geo' in dim_positions and dim_positions['geo'] < len(key_parts):
                    geo_idx = dim_positions['geo']
                    country_code = key_parts[geo_idx]
                    
                if 'time' in dim_positions and dim_positions['time'] < len(key_parts):
                    time_idx = dim_positions['time']
                    time_period = key_parts[time_idx]
                    
                if country_code and time_period:
                    record = {
                        'country_code': country_code,
                        'country_name': geo_labels.get(country_code, country_code),
                        'time_period': time_period,
                        'value': float(value) if value is not None else None
                    }
                    records.append(record)
        
        # Jeśli poprzednie metody nie dały wyników, użyj metody bezpośredniego mapowania
        if not records:
            # Iteruj po wszystkich krajach i latach
            for country_code in geo_indices.keys():
                for time_period in time_indices.keys():
                    found_value = None
                    
                    # Przeszukaj wszystkie wartości w poszukiwaniu pasujących
                    for key, value in values.items():
                        if value is None:
                            continue
                        
                        # Sprawdź, czy klucz odnosi się do tego kraju i okresu
                        matches_country = False
                        matches_time = False
                        
                        if ":" in key:
                            key_parts = key.split(":")
                            for part in key_parts:
                                if part == country_code:
                                    matches_country = True
                                if part == time_period:
                                    matches_time = True
                        
                        if matches_country and matches_time:
                            found_value = value
                            break
                    
                    if found_value is not None:
                        record = {
                            'country_code': country_code,
                            'country_name': geo_labels.get(country_code, country_code),
                            'time_period': time_period,
                            'value': float(found_value)
                        }
                        records.append(record)
        
        return records
    
    def _parse_generic_data(self, values: Dict, geo_indices: Dict, geo_labels: Dict, time_indices: Dict, dataset_name: str) -> List[Dict]:
        """Ogólne parsowanie danych"""
        records = []
        country_codes = list(geo_indices.keys())
        time_codes = list(time_indices.keys())
        
        for value_key, value in values.items():
            try:
                key_num = int(value_key)
                
                if len(time_codes) > 0:
                    country_idx = key_num // len(time_codes)
                    time_idx = key_num % len(time_codes)
                    
                    if country_idx < len(country_codes) and time_idx < len(time_codes):
                        country_code = country_codes[country_idx]
                        time_period = time_codes[time_idx]
                        
                        record = {
                            'country_code': country_code,
                            'country_name': geo_labels.get(country_code, country_code),
                            'time_period': time_period,
                            'value': float(value) if value is not None else None
                        }
                        records.append(record)
            except (ValueError, IndexError):
                continue
        
        return records
    
    def _create_empty_dataframe_with_structure(self, dataset_name: str) -> pd.DataFrame:
        """Tworzy pustą ramkę danych z odpowiednią strukturą"""
        records = []
        current_year = datetime.now().year
        
        for country_code in self.priority_countries:
            if country_code in self.country_mapping:
                for year in range(2020, current_year + 1):
                    if dataset_name == 'electricity_prices':
                        for semester in ['S1', 'S2']:
                            records.append({
                                'country_code': country_code,
                                'country_name': self.country_mapping[country_code],
                                'time_period': f"{year}-{semester}",
                                'value': None,
                                'dataset': dataset_name
                            })
                    else:
                        records.append({
                            'country_code': country_code,
                            'country_name': self.country_mapping[country_code],
                            'time_period': str(year),
                            'value': None,
                            'dataset': dataset_name
                        })
        
        return pd.DataFrame(records)
    
    def get_population_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o populacji"""
        self.logger.info("Fetching population data from Eurostat")
        
        dataset_info = self.datasets['population']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('population')
        
        df = self._parse_eurostat_response(response_data, 'population')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_gdp_per_capita_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o PKB per capita z fallback"""
        self.logger.info("Fetching GDP per capita data from Eurostat")
        
        dataset_info = self.datasets['gdp_per_capita']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('gdp_per_capita')
        
        df = self._parse_eurostat_response(response_data, 'gdp_per_capita')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_electricity_prices_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o cenach energii elektrycznej"""
        self.logger.info("Fetching electricity prices data from Eurostat")
        
        dataset_info = self.datasets['electricity_prices']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('electricity_prices')
        
        df = self._parse_eurostat_response(response_data, 'electricity_prices')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
        
    def get_energy_intensity_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o intensywności energetycznej z fallback"""
        self.logger.info("Fetching energy intensity data from Eurostat")
        
        dataset_info = self.datasets['energy_intensity']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('energy_intensity')
        
        # Parsuj odpowiedź
        df = self._parse_eurostat_response(response_data, 'energy_intensity')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_unemployment_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o stopie bezrobocia z fallback"""
        self.logger.info("Fetching unemployment rate data from Eurostat")
        
        dataset_info = self.datasets['unemployment_rate']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('unemployment_rate')
        
        df = self._parse_eurostat_response(response_data, 'unemployment_rate')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_poverty_by_urbanization_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o urbanizacji"""
        self.logger.info("Fetching urbanization data from Eurostat")
        
        dataset_info = self.datasets['poverty_by_degree_of_urbanization']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('poverty_by_degree_of_urbanization')
        
        df = self._parse_eurostat_response(response_data, 'poverty_by_degree_of_urbanization')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_service_sector_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o sektorze usług"""
        self.logger.info("Fetching service sector data from Eurostat")
        
        dataset_info = self.datasets['service_sector']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('service_sector')
        
        df = self._parse_eurostat_response(response_data, 'service_sector')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_industry_sector_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o sektorze przemysłowym"""
        self.logger.info("Fetching industry sector data from Eurostat")
        
        dataset_info = self.datasets['industry_sector']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('industry_sector')
        
        df = self._parse_eurostat_response(response_data, 'industry_sector')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_household_size_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o wielkości gospodarstw domowych"""
        self.logger.info("Fetching household size data from Eurostat")
        
        dataset_info = self.datasets['household_size']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('household_size')
        
        df = self._parse_eurostat_response(response_data, 'household_size')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_energy_poverty_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o ubóstwie energetycznym"""
        self.logger.info("Fetching energy poverty data from Eurostat")
        
        dataset_info = self.datasets['energy_poverty']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('energy_poverty')
        
        df = self._parse_eurostat_response(response_data, 'energy_poverty')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def get_heating_systems_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o systemach grzewczych"""
        self.logger.info("Fetching heating systems data from Eurostat")
        
        dataset_info = self.datasets['heating_systems']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('heating_systems')
        
        df = self._parse_eurostat_response(response_data, 'heating_systems')
        
        if not df.empty and countries:
            df = df[df['country_code'].isin(countries)]
        
        return df
    
    def extract_all_socioeconomic_data(self, since_year: int = 2020, countries: List[str] = None) -> Dict[str, pd.DataFrame]:
        """Pobieranie wszystkich danych socjoekonomicznych"""
        
        if countries is None:
            countries = self.priority_countries
        
        self.logger.info(f"Extracting all socioeconomic data for countries: {countries}")
        
        raw_data = {}
        
        # Podstawowe dane
        raw_data['population'] = self.get_population_data(countries, since_year)
        raw_data['gdp_per_capita'] = self.get_gdp_per_capita_data(countries, since_year)
        raw_data['electricity_prices'] = self.get_electricity_prices_data(countries, since_year)
        raw_data['energy_intensity'] = self.get_energy_intensity_data(countries, since_year)
        raw_data['unemployment_rate'] = self.get_unemployment_data(countries, since_year)
        
        # Dodatkowe dane dla lepszych profili socjoekonomicznych
        raw_data['poverty_by_degree_of_urbanization'] = self.get_poverty_by_urbanization_data(countries, since_year)
        raw_data['service_sector'] = self.get_service_sector_data(countries, since_year)
        raw_data['industry_sector'] = self.get_industry_sector_data(countries, since_year)
        raw_data['household_size'] = self.get_household_size_data(countries, since_year)
        raw_data['energy_poverty'] = self.get_energy_poverty_data(countries, since_year)
        raw_data['heating_systems'] = self.get_heating_systems_data(countries, since_year)
        
        return raw_data
    
    def prepare_integrated_socioeconomic_data(self, raw_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Przygotowanie zintegrowanych danych socjoekonomicznych"""
        
        self.logger.info("Preparing integrated socioeconomic data")
        
        integrated_records = []
        
        # Uzyskaj listę wszystkich krajów i okresów
        all_countries = set()
        all_periods = set()
        
        for dataset_name, df in raw_data.items():
            if not df.empty:
                all_countries.update(df['country_code'].unique())
                all_periods.update(df['time_period'].unique())
        
        if not all_countries:
            all_countries = set(self.priority_countries)
        
        if not all_periods:
            current_year = datetime.now().year
            all_periods = {str(year) for year in range(2020, current_year + 1)}
        
        # Dla każdego kraju i okresu utwórz zintegrowany rekord
        for country in all_countries:
            country_name = self.country_mapping.get(country, country)
            
            # Grupuj po latach
            years = set()
            for period in all_periods:
                if '-S' in period:
                    year = period.split('-')[0]
                    years.add(year)
                else:
                    years.add(period)
            
            for year in years:
                record = {
                    'country_code': country,
                    'country_name': country_name,
                    'year': int(year),
                    'population': None,
                    'gdp_per_capita': None,
                    'electricity_price_avg': None,
                    'energy_intensity': None,
                    'unemployment_rate': None,
                    'poverty_by_degree_of_urbanization': None,  # Poprawna nazwa
                    'service_sector_percentage': None,
                    'industry_sector_percentage': None,
                    'avg_household_size': None,
                    'energy_poverty_rate': None,
                    'primary_heating_type': None,
                    'data_quality_score': 0
                }
                
                # Populacja
                pop_data = raw_data.get('population', pd.DataFrame())
                if not pop_data.empty:
                    pop_value = pop_data[
                        (pop_data['country_code'] == country) & 
                        (pop_data['time_period'] == year)
                    ]
                    if not pop_value.empty and pop_value.iloc[0]['value'] is not None:
                        record['population'] = pop_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # PKB per capita
                gdp_data = raw_data.get('gdp_per_capita', pd.DataFrame())
                if not gdp_data.empty:
                    gdp_value = gdp_data[
                        (gdp_data['country_code'] == country) & 
                        (gdp_data['time_period'] == year)
                    ]
                    if not gdp_value.empty and gdp_value.iloc[0]['value'] is not None:
                        record['gdp_per_capita'] = gdp_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Średnia cena energii
                elec_data = raw_data.get('electricity_prices', pd.DataFrame())
                if not elec_data.empty:
                    elec_values = elec_data[
                        (elec_data['country_code'] == country) & 
                        (elec_data['time_period'].str.startswith(year))
                    ]
                    if not elec_values.empty:
                        valid_values = elec_values[elec_values['value'].notna()]['value']
                        if len(valid_values) > 0:
                            record['electricity_price_avg'] = valid_values.mean()
                            record['data_quality_score'] += 1
                
                # Intensywność energetyczna
                energy_data = raw_data.get('energy_intensity', pd.DataFrame())
                if not energy_data.empty:
                    energy_value = energy_data[
                        (energy_data['country_code'] == country) & 
                        (energy_data['time_period'] == year)
                    ]
                    if not energy_value.empty and energy_value.iloc[0]['value'] is not None:
                        record['energy_intensity'] = energy_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Stopa bezrobocia
                unemp_data = raw_data.get('unemployment_rate', pd.DataFrame())
                if not unemp_data.empty:
                    unemp_value = unemp_data[
                        (unemp_data['country_code'] == country) & 
                        (unemp_data['time_period'] == year)
                    ]
                    if not unemp_value.empty and unemp_value.iloc[0]['value'] is not None:
                        record['unemployment_rate'] = unemp_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Stopień ubóstwa według urbanizacji - poprawna nazwa
                urban_data = raw_data.get('poverty_by_degree_of_urbanization', pd.DataFrame())
                if not urban_data.empty:
                    urban_value = urban_data[
                        (urban_data['country_code'] == country) & 
                        (urban_data['time_period'] == year)
                    ]
                    if not urban_value.empty and urban_value.iloc[0]['value'] is not None:
                        record['poverty_by_degree_of_urbanization'] = urban_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Udział sektora usług
                service_data = raw_data.get('service_sector', pd.DataFrame())
                if not service_data.empty:
                    service_value = service_data[
                        (service_data['country_code'] == country) & 
                        (service_data['time_period'] == year)
                    ]
                    if not service_value.empty and service_value.iloc[0]['value'] is not None:
                        record['service_sector_percentage'] = service_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Udział sektora przemysłowego
                industry_data = raw_data.get('industry_sector', pd.DataFrame())
                if not industry_data.empty:
                    industry_value = industry_data[
                        (industry_data['country_code'] == country) & 
                        (industry_data['time_period'] == year)
                    ]
                    if not industry_value.empty and industry_value.iloc[0]['value'] is not None:
                        record['industry_sector_percentage'] = industry_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Wielkość gospodarstwa domowego
                household_data = raw_data.get('household_size', pd.DataFrame())
                if not household_data.empty:
                    household_value = household_data[
                        (household_data['country_code'] == country) & 
                        (household_data['time_period'] == year)
                    ]
                    if not household_value.empty and household_value.iloc[0]['value'] is not None:
                        record['avg_household_size'] = household_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Ubóstwo energetyczne
                poverty_data = raw_data.get('energy_poverty', pd.DataFrame())
                if not poverty_data.empty:
                    poverty_value = poverty_data[
                        (poverty_data['country_code'] == country) & 
                        (poverty_data['time_period'] == year)
                    ]
                    if not poverty_value.empty and poverty_value.iloc[0]['value'] is not None:
                        record['energy_poverty_rate'] = poverty_value.iloc[0]['value']
                        record['data_quality_score'] += 1
                
                # Typ ogrzewania - używamy ostatnich dostępnych danych
                heating_data = raw_data.get('heating_systems', pd.DataFrame())
                if not heating_data.empty:
                    # Filtrujemy po kraju i bierzemy najnowszy rok
                    country_heating = heating_data[heating_data['country_code'] == country]
                    if not country_heating.empty:
                        # Sortowanie wg roku i wybór najnowszego
                        country_heating = country_heating.sort_values('time_period', ascending=False)
                        if country_heating.iloc[0]['value'] is not None:
                            record['primary_heating_type'] = self._determine_primary_heating(country, country_heating.iloc[0]['value'])
                            record['data_quality_score'] += 1
                
                integrated_records.append(record)
        
        df_integrated = pd.DataFrame(integrated_records)
        
        if not df_integrated.empty:
            df_integrated = df_integrated.sort_values(['country_code', 'year'])
        
        self.logger.info(f"Created {len(df_integrated)} integrated records")
        
        # Zastąp NaN wartościami None
        df_integrated = df_integrated.where(pd.notna(df_integrated), None)
        
        return df_integrated
    
    def _fill_missing_values(self, record: Dict):
        """Uzupełnia brakujące wartości w rekordzie socjoekonomicznym"""
        pass
        
    
    def _determine_primary_heating(self, country_code: str, heating_value=None) -> str:
        """
        Określa główny typ ogrzewania na podstawie kraju i wartości z danych
        
        Args:
            country_code: Kod kraju
            heating_value: Wartość z danych o ogrzewaniu
            
        Returns:
            Główny typ ogrzewania jako string
        """
        # Mapa podstawowych typów ogrzewania dla krajów
        default_heating = {
            'PL': 'District Heating', 'DE': 'Natural Gas', 'FR': 'Electricity',
            'ES': 'Natural Gas', 'IT': 'Natural Gas', 'CZ': 'District Heating',
            'SK': 'Natural Gas', 'HU': 'District Heating', 'AT': 'Natural Gas',
            'NL': 'Natural Gas', 'BE': 'Natural Gas', 'DK': 'District Heating',
            'SE': 'District Heating', 'FI': 'District Heating', 'LT': 'District Heating',
            'LV': 'District Heating', 'EE': 'District Heating', 'RO': 'Natural Gas',
            'BG': 'Electricity', 'GR': 'Electricity', 'PT': 'Electricity',
            'IE': 'Natural Gas', 'LU': 'Natural Gas', 'SI': 'Natural Gas',
            'HR': 'Natural Gas', 'MT': 'Electricity', 'CY': 'Electricity'
        }
        
        # Upewnij się, że zwracamy string
        if heating_value is not None:
            try:
                # Spróbuj przekonwertować heating_value na float, jeśli to możliwe
                float_value = float(heating_value)
                # Tutaj możesz dodać logikę interpretacji wartości numerycznych
                # Na razie po prostu zwróć domyślny typ ogrzewania
                return default_heating.get(country_code, 'Natural Gas')
            except (ValueError, TypeError):
                # Jeśli nie da się przekonwertować, to heating_value jest prawdopodobnie stringiem
                # W tej sytuacji także zwracamy domyślny typ ogrzewania
                return default_heating.get(country_code, 'Natural Gas')
        
        # Jeśli heating_value jest None, zwróć domyślny typ ogrzewania
        return default_heating.get(country_code, 'Natural Gas')
    
    def export_to_csv(self, raw_data: Dict[str, pd.DataFrame], integrated_data: pd.DataFrame, 
                     output_dir: str = "eurostat_export") -> bool:
        """Eksport danych do plików CSV"""
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            for dataset_name, df in raw_data.items():
                if not df.empty:
                    filename = os.path.join(output_dir, f"eurostat_raw_{dataset_name}.csv")
                    df.to_csv(filename, index=False, encoding='utf-8')
                    self.logger.info(f"Exported {len(df)} records to {filename}")
            
            if not integrated_data.empty:
                filename = os.path.join(output_dir, "eurostat_integrated.csv")
                integrated_data.to_csv(filename, index=False, encoding='utf-8')
                self.logger.info(f"Exported {len(integrated_data)} integrated records to {filename}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {str(e)}")
            return False
    
    def save_to_staging(self, raw_data: Dict[str, pd.DataFrame], integrated_data: pd.DataFrame) -> bool:
        """Zapis danych do tabel staging w bazie danych - z usuwaniem i ponownym tworzeniem tabel"""
        
        if not self.connection_string:
            self.logger.warning("No connection string provided - skipping database save")
            return True
        
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                
                # Usuń istniejące tabele
                self.logger.info("Dropping existing staging tables")
                cursor.execute("IF OBJECT_ID('staging_eurostat_raw', 'U') IS NOT NULL DROP TABLE staging_eurostat_raw")
                cursor.execute("IF OBJECT_ID('staging_eurostat_integrated', 'U') IS NOT NULL DROP TABLE staging_eurostat_integrated")
                
                # Utwórz tabele na nowo
                self.logger.info("Creating new staging tables")
                # Tabela staging dla surowych danych
                cursor.execute("""
                    CREATE TABLE staging_eurostat_raw (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        dataset_name NVARCHAR(100),
                        country_code NVARCHAR(10),
                        country_name NVARCHAR(100),
                        time_period NVARCHAR(20),
                        value FLOAT,
                        extraction_date DATETIME DEFAULT GETDATE()
                    )
                """)
                
                # Tabela staging dla zintegrowanych danych
                cursor.execute("""
                    CREATE TABLE staging_eurostat_integrated (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        country_code NVARCHAR(10),
                        country_name NVARCHAR(100),
                        year INT,
                        population FLOAT,
                        gdp_per_capita FLOAT,
                        electricity_price_avg FLOAT,
                        energy_intensity FLOAT,
                        unemployment_rate FLOAT,
                        poverty_by_degree_of_urbanization FLOAT,
                        service_sector_percentage FLOAT,
                        industry_sector_percentage FLOAT,
                        avg_household_size FLOAT,
                        energy_poverty_rate FLOAT,
                        primary_heating_type NVARCHAR(50),
                        data_quality_score INT,
                        extraction_date DATETIME DEFAULT GETDATE()
                    )
                """)
                
                # Wstaw surowe dane
                raw_insert_count = 0
                for dataset_name, df in raw_data.items():
                    if not df.empty:
                        for _, row in df.iterrows():
                            cursor.execute("""
                                INSERT INTO staging_eurostat_raw 
                                (dataset_name, country_code, country_name, time_period, value)
                                VALUES (?, ?, ?, ?, ?)
                            """, dataset_name, row['country_code'], row['country_name'], 
                               row['time_period'], row['value'])
                            raw_insert_count += 1
                
                # Wstaw zintegrowane dane
                integrated_insert_count = 0
                if not integrated_data.empty:
                    for _, row in integrated_data.iterrows():
                        # Upewnij się, że wartości są odpowiedniego typu lub NULL
                        population = float(row['population']) if pd.notna(row['population']) else None
                        gdp_per_capita = float(row['gdp_per_capita']) if pd.notna(row['gdp_per_capita']) else None
                        electricity_price_avg = float(row['electricity_price_avg']) if pd.notna(row['electricity_price_avg']) else None
                        energy_intensity = float(row['energy_intensity']) if pd.notna(row['energy_intensity']) else None
                        unemployment_rate = float(row['unemployment_rate']) if pd.notna(row['unemployment_rate']) else None
                        poverty_by_degree_of_urbanization = float(row['poverty_by_degree_of_urbanization']) if pd.notna(row['poverty_by_degree_of_urbanization']) else None
                        service_sector_percentage = float(row['service_sector_percentage']) if pd.notna(row['service_sector_percentage']) else None
                        industry_sector_percentage = float(row['industry_sector_percentage']) if pd.notna(row['industry_sector_percentage']) else None
                        avg_household_size = float(row['avg_household_size']) if pd.notna(row['avg_household_size']) else None
                        energy_poverty_rate = float(row['energy_poverty_rate']) if pd.notna(row['energy_poverty_rate']) else None
                        
                        # Dodajmy debugowanie, aby zobaczyć, co dokładnie powoduje problem
                        self.logger.debug(f"Row data types: primary_heating_type={type(row['primary_heating_type'])}, value={row['primary_heating_type']}")
                        
                        cursor.execute("""
                            INSERT INTO staging_eurostat_integrated 
                            (country_code, country_name, year, population, gdp_per_capita, 
                            electricity_price_avg, energy_intensity, unemployment_rate, 
                            poverty_by_degree_of_urbanization, service_sector_percentage, 
                            industry_sector_percentage, avg_household_size, energy_poverty_rate, 
                            primary_heating_type, data_quality_score)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, 
                        row['country_code'], 
                        row['country_name'], 
                        int(row['year']), 
                        population, 
                        gdp_per_capita,
                        electricity_price_avg, 
                        energy_intensity, 
                        unemployment_rate, 
                        poverty_by_degree_of_urbanization, 
                        service_sector_percentage,
                        industry_sector_percentage, 
                        avg_household_size, 
                        energy_poverty_rate, 
                        row['primary_heating_type'], 
                        int(row['data_quality_score']))
                        integrated_insert_count += 1
                
                conn.commit()
                
                self.logger.info(f"Successfully saved {raw_insert_count} raw records and {integrated_insert_count} integrated records to staging tables")
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving to database: {str(e)}")
            traceback.print_exc()
            return False
    
    def get_data_summary(self, raw_data: Dict[str, pd.DataFrame], integrated_data: pd.DataFrame) -> Dict:
        """Generuje podsumowanie pobranych danych"""
        
        summary = {
            'extraction_timestamp': datetime.now().isoformat(),
            'raw_data_stats': {},
            'integrated_data_stats': {},
            'data_quality_overview': {}
        }
        
        # Statystyki surowych danych
        total_raw_records = 0
        for dataset_name, df in raw_data.items():
            record_count = len(df) if not df.empty else 0
            non_null_count = len(df[df['value'].notna()]) if not df.empty else 0
            
            summary['raw_data_stats'][dataset_name] = {
                'total_records': record_count,
                'records_with_data': non_null_count,
                'data_coverage_pct': (non_null_count / record_count * 100) if record_count > 0 else 0
            }
            total_raw_records += record_count
        
        summary['raw_data_stats']['total'] = total_raw_records
        
        # Statystyki zintegrowanych danych
        if not integrated_data.empty:
            summary['integrated_data_stats'] = {
                'total_country_year_profiles': len(integrated_data),
                'countries_covered': integrated_data['country_code'].nunique(),
                'years_covered': integrated_data['year'].nunique(),
                'avg_data_quality_score': integrated_data['data_quality_score'].mean(),
                'profiles_with_complete_data': len(integrated_data[integrated_data['data_quality_score'] >= 10])
            }
            
            # Analiza jakości danych
            quality_distribution = integrated_data['data_quality_score'].value_counts().to_dict()
            summary['data_quality_overview'] = {
                'quality_score_distribution': quality_distribution,
                'countries_by_avg_quality': integrated_data.groupby('country_code')['data_quality_score'].mean().to_dict()
            }
        
        return summary


def print_summary_to_console(raw_data, integrated_data):
    """Wyświetla podsumowanie pobranych danych w konsoli"""
    print("\n===== EUROSTAT DATA EXTRACTION SUMMARY =====")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nRaw Data Stats:")
    
    total_records = 0
    for dataset_name, df in raw_data.items():
        record_count = len(df) if not df.empty else 0
        data_count = len(df[df['value'].notna()]) if not df.empty else 0
        total_records += record_count
        coverage = (data_count / record_count * 100) if record_count > 0 else 0
        print(f"- {dataset_name}: {record_count} records ({data_count} with data, {coverage:.1f}% coverage)")
    
    print(f"\nTotal raw records: {total_records}")
    print(f"Countries with profiles: {integrated_data['country_code'].nunique() if not integrated_data.empty else 0}")
    
    # Podsumowanie dla każdego kraju
    if not integrated_data.empty and len(integrated_data) > 0:
        print("\nData availability by country:")
        for country_code in integrated_data['country_code'].unique():
            country_data = integrated_data[integrated_data['country_code'] == country_code]
            avg_quality = country_data['data_quality_score'].mean()
            country_name = country_data.iloc[0]['country_name']
            
            print(f"- {country_name} ({country_code}): Avg quality score {avg_quality:.1f}/11.0")
    
    print("\n============================================")


def main():
    """Główna funkcja wywoływana przez SSIS"""
    import sys
    import os
    from datetime import datetime
    
    # Konfiguracja logowania
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("EurostatMain")
    
    # Parametry z SSIS lub zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Jeśli nie podano connection_string, użyj domyślnego
    if not connection_string:
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        logger.info(f"Using default connection string: {connection_string}")
    
    # Domyślnie pobieramy dane od 2020 roku
    since_year = 2020
    
    # Parametr z argumentów linii komend (opcjonalnie)
    if len(sys.argv) > 1:
        try:
            since_year = int(sys.argv[1])
        except ValueError:
            logger.warning(f"Invalid format of year: {sys.argv[1]}, using default: 2020")
            since_year = 2020
    
    # Lista krajów do przetworzenia (opcjonalnie z argumentów)
    countries_to_process = None
    if len(sys.argv) > 2:
        countries_arg = sys.argv[2]
        countries_to_process = [c.strip().upper() for c in countries_arg.split(',')]
    
    try:
        # Inicjalizacja klienta
        client = EurostatClient(connection_string)
        
        # Ekstrakcja surowych danych
        logger.info(f"Starting Eurostat data extraction from year {since_year}")
        raw_data = client.extract_all_socioeconomic_data(since_year, countries_to_process)
        
        # Przygotowanie zintegrowanych danych
        logger.info("Preparing integrated data...")
        integrated_data = client.prepare_integrated_socioeconomic_data(raw_data)
        
        # Wyświetlenie podsumowania
        print_summary_to_console(raw_data, integrated_data)
        
        # Eksport do CSV (zawsze)
        logger.info("Exporting data to CSV files...")
        csv_success = client.export_to_csv(raw_data, integrated_data)
        
        # Zapis do staging tylko jeśli mamy connection string
        db_success = True
        if connection_string:
            logger.info("Saving data to staging tables...")
            db_success = client.save_to_staging(raw_data, integrated_data)
        else:
            logger.warning("No database connection string provided - skipping database save")
        
        # Generuj szczegółowe podsumowanie
        summary = client.get_data_summary(raw_data, integrated_data)
        
        # Zapisz podsumowanie do pliku JSON
        try:
            with open('eurostat_extraction_summary.json', 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info("Detailed summary saved to eurostat_extraction_summary.json")
        except Exception as e:
            logger.warning(f"Could not save summary to file: {str(e)}")
        
        if csv_success and db_success:
            logger.info("Eurostat data extraction completed successfully")
            sys.exit(0)
        else:
            logger.warning("Eurostat data extraction completed with warnings")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()