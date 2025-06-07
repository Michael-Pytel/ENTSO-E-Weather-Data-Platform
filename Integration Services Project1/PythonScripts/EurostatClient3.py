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
        
        # KONFIGURACJA DATASETÓW - POPRAWIONA WERSJA
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
                'code': 'sdg_07_30',
                'description': 'Energy intensity of the economy',
                'params': {
                    'unit': 'EUR_KGOE'  # Euro per kilogram oil equivalent
                },
                'alternative_units': ['PPS_KGOE']  # Alternatywna jednostka PPS
            },
            'unemployment_rate': {
                'code': 'une_rt_a',
                'description': 'Unemployment rate by sex and age',
                'params': {
                    'sex': 'T',      # Total
                    'age': 'Y15-74', # Age 15-74
                    'unit': 'PC_ACT' # Percentage of active population
                },
                'alternative_units': ['THS_PER'],
                'alternative_age_params': ['Y_LT25', 'Y_GE25']  # Fallback age parameter
            },
            # POPRAWIONO: Użycie właściwego datasetu dla urbanizacji
            'urbanization': {
                'code': 'urb_lpop1',  # Poprawny dataset dla odsetka ludności miejskiej
                'description': 'Urban population percentage',
                'params': {
                    'unit': 'PC',     # Percentage
                    'cities': 'TOTAL' # Total urban areas
                },
                'fallback_dataset': {
                    'code': 'demo_gind',
                    'params': {
                        'indic_de': 'POPSHARE',
                        'unit': 'PC'
                    }
                }
            },
            # POPRAWIONO: Parametry dla sektora usług
            'service_sector': {
                'code': 'nama_10_a10',
                'description': 'Service sector percentage',
                'params': {
                    'unit': 'PC_TOT',     # Percentage of total
                    'nace_r2': 'G-U',     # Services sectors G through U
                    'na_item': 'B1G'      # Gross value added
                },
                'alternative_params': [
                    {
                        'unit': 'PC_TOT',
                        'nace_r2': 'G-J',  # Try different groupings
                        'na_item': 'B1G'
                    },
                    {
                        'unit': 'PC_TOT',
                        'nace_r2': 'G-N',
                        'na_item': 'B1G'
                    }
                ]
            },
            # POPRAWIONO: Parametry dla sektora przemysłowego
            'industry_sector': {
                'code': 'nama_10_a10',
                'description': 'Industry sector percentage',
                'params': {
                    'unit': 'PC_TOT',     # Percentage of total
                    'nace_r2': 'B-E',     # Industry sectors B through E
                    'na_item': 'B1G'      # Gross value added
                },
                'alternative_params': [
                    {
                        'unit': 'PC_TOT',
                        'nace_r2': 'B-F',  # Try including construction
                        'na_item': 'B1G'
                    }
                ]
            },
            # POPRAWIONO: Użycie właściwego datasetu dla wielkości gospodarstw domowych
            'household_size': {
                'code': 'ilc_lvph01',  # Dataset z średnią wielkością gospodarstw domowych
                'description': 'Average household size',
                'params': {
                    'unit': 'AVG',     # Average
                    'hhtyp': 'TOTAL'   # All household types
                },
                'fallback_dataset': [
                    {
                        'code': 'hbs_car_t313',
                        'params': {
                            'unit': 'NR',
                            'coicop': 'TOTAL'
                        }
                    }
                ]
            },
            'energy_poverty': {
                'code': 'ilc_mdes01',
                'description': 'Energy poverty rate - inability to keep home adequately warm',
                'params': {
                    'unit': 'PC',     # Percentage
                    'hhtyp': 'TOTAL'  # Total households
                },
                # Dodano fallback w przypadku błędu
                'fallback_dataset': {
                    'code': 'sdg_07_60',
                    'params': {
                        'unit': 'PC'
                    }
                }
            },
            # POPRAWIONO: Parametry dla systemów grzewczych
            'heating_systems': {
                'code': 'nrg_d_hhq',  # Dataset dla systemów grzewczych w gospodarstwach domowych
                'description': 'Primary heating systems and energy usage',
                'params': {
                    'unit': 'PC',              # Percentage
                    'nrg_bal': 'FC_OTH_HH_E'   # Final consumption - households - energy use
                },
                'fallback_dataset': [
                    {
                        'code': 'nrg_pc_202',
                        'params': {
                            'unit': 'KWH',
                            'product': '4100', # Natural gas
                            'consom': 'MWH_LT20' # Residential usage
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
                        
                        # Walidacja i konwersja wartości
                        parsed_value = None
                        if value is not None:
                            try:
                                parsed_value = float(value)
                                
                                # Specjalne przetwarzanie dla określonych datasetów
                                if dataset_name == 'household_size' and parsed_value > 10:
                                    # Zbyt duża wartość dla średniej wielkości gospodarstwa
                                    self.logger.warning(f"Unrealistic household size: {parsed_value} for {country_code}, {time_period}")
                                    parsed_value = None
                                elif dataset_name in ['service_sector_percentage', 'industry_sector_percentage'] and parsed_value > 100:
                                    # Nieprawidłowa wartość procentowa
                                    self.logger.warning(f"Invalid percentage for {dataset_name}: {parsed_value} for {country_code}, {time_period}")
                                    parsed_value = None
                                
                            except (ValueError, TypeError):
                                self.logger.warning(f"Could not parse value for {dataset_name}: {value}")
                        
                        record = {
                            'country_code': country_code,
                            'country_name': geo_labels.get(country_code, country_code),
                            'time_period': time_period,
                            'value': parsed_value
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
    
    def get_urbanization_data(self, countries: List[str] = None, since_year: int = 2020) -> pd.DataFrame:
        """Pobieranie danych o urbanizacji"""
        self.logger.info("Fetching urbanization data from Eurostat")
        
        dataset_info = self.datasets['urbanization']
        response_data = self._make_request_with_fallback(dataset_info, countries, since_year)
        
        if not response_data:
            return self._create_empty_dataframe_with_structure('urbanization')
        
        df = self._parse_eurostat_response(response_data, 'urbanization')
        
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