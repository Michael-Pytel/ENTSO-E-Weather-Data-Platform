"""
OpenMeteoClient.py
Klient API do komunikacji z Open-Meteo
Pobiera historyczne dane pogodowe i wskaźniki klimatyczne
Rozszerzony o wszystkie kraje Unii Europejskiej
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Tuple
import pyodbc
import json
import os
import sys

class OpenMeteoClient:
    """Klient API dla Open-Meteo Weather Service"""
    
    def __init__(self, connection_string: str):
        """
        Inicjalizacja klienta Open-Meteo
        
        Args:
            connection_string: String połączenia z bazą danych
        """
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.climate_url = "https://climate-api.open-meteo.com/v1/climate"
        self.connection_string = connection_string
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Rozszerzona mapa lokalizacji - wszystkie kraje UE z reprezentatywnymi miastami
        self.weather_locations = {
            'AT': {  # Austria
                'name': 'Austria',
                'subzones': [
                    {'name': 'Vienna', 'lat': 48.2082, 'lon': 16.3738, 'subzone_code': 'AT-VIE'},
                    {'name': 'Salzburg', 'lat': 47.8095, 'lon': 13.0550, 'subzone_code': 'AT-SAL'},
                    {'name': 'Innsbruck', 'lat': 47.2692, 'lon': 11.4041, 'subzone_code': 'AT-INN'}
                ]
            },
            'BE': {  # Belgium
                'name': 'Belgium',
                'subzones': [
                    {'name': 'Brussels', 'lat': 50.8503, 'lon': 4.3517, 'subzone_code': 'BE-BRU'},
                    {'name': 'Antwerp', 'lat': 51.2194, 'lon': 4.4025, 'subzone_code': 'BE-ANT'},
                    {'name': 'Ghent', 'lat': 51.0543, 'lon': 3.7174, 'subzone_code': 'BE-GHE'}
                ]
            },
            'BG': {  # Bulgaria
                'name': 'Bulgaria',
                'subzones': [
                    {'name': 'Sofia', 'lat': 42.6977, 'lon': 23.3219, 'subzone_code': 'BG-SOF'},
                    {'name': 'Plovdiv', 'lat': 42.1354, 'lon': 24.7453, 'subzone_code': 'BG-PLO'},
                    {'name': 'Varna', 'lat': 43.2141, 'lon': 27.9147, 'subzone_code': 'BG-VAR'}
                ]
            },
            'HR': {  # Croatia
                'name': 'Croatia',
                'subzones': [
                    {'name': 'Zagreb', 'lat': 45.8150, 'lon': 15.9819, 'subzone_code': 'HR-ZAG'},
                    {'name': 'Split', 'lat': 43.5081, 'lon': 16.4402, 'subzone_code': 'HR-SPL'},
                    {'name': 'Rijeka', 'lat': 45.3271, 'lon': 14.4422, 'subzone_code': 'HR-RIJ'}
                ]
            },
            'CY': {  # Cyprus
                'name': 'Cyprus',
                'subzones': [
                    {'name': 'Nicosia', 'lat': 35.1856, 'lon': 33.3823, 'subzone_code': 'CY-NIC'},
                    {'name': 'Limassol', 'lat': 34.6751, 'lon': 33.0392, 'subzone_code': 'CY-LIM'},
                    {'name': 'Larnaca', 'lat': 34.9086, 'lon': 33.6290, 'subzone_code': 'CY-LAR'}
                ]
            },
            'CZ': {  # Czech Republic
                'name': 'Czech Republic',
                'subzones': [
                    {'name': 'Prague', 'lat': 50.0755, 'lon': 14.4378, 'subzone_code': 'CZ-PRG'},
                    {'name': 'Brno', 'lat': 49.1951, 'lon': 16.6068, 'subzone_code': 'CZ-BRN'},
                    {'name': 'Ostrava', 'lat': 49.8209, 'lon': 18.2625, 'subzone_code': 'CZ-OST'}
                ]
            },
            'DK': {  # Denmark
                'name': 'Denmark',
                'subzones': [
                    {'name': 'Copenhagen', 'lat': 55.6761, 'lon': 12.5683, 'subzone_code': 'DK-COP'},
                    {'name': 'Aarhus', 'lat': 56.1629, 'lon': 10.2039, 'subzone_code': 'DK-AAR'},
                    {'name': 'Aalborg', 'lat': 57.0488, 'lon': 9.9217, 'subzone_code': 'DK-AAL'}
                ]
            },
            'EE': {  # Estonia
                'name': 'Estonia',
                'subzones': [
                    {'name': 'Tallinn', 'lat': 59.4370, 'lon': 24.7536, 'subzone_code': 'EE-TAL'},
                    {'name': 'Tartu', 'lat': 58.3780, 'lon': 26.7290, 'subzone_code': 'EE-TAR'},
                    {'name': 'Narva', 'lat': 59.3772, 'lon': 28.1903, 'subzone_code': 'EE-NAR'}
                ]
            },
            'FI': {  # Finland
                'name': 'Finland',
                'subzones': [
                    {'name': 'Helsinki', 'lat': 60.1699, 'lon': 24.9384, 'subzone_code': 'FI-HEL'},
                    {'name': 'Tampere', 'lat': 61.4991, 'lon': 23.7871, 'subzone_code': 'FI-TAM'},
                    {'name': 'Turku', 'lat': 60.4518, 'lon': 22.2666, 'subzone_code': 'FI-TUR'}
                ]
            },
            'FR': {  # France
                'name': 'France',
                'subzones': [
                    {'name': 'Paris', 'lat': 48.8566, 'lon': 2.3522, 'subzone_code': 'FR-PAR'},
                    {'name': 'Lyon', 'lat': 45.7640, 'lon': 4.8357, 'subzone_code': 'FR-LYO'},
                    {'name': 'Marseille', 'lat': 43.2965, 'lon': 5.3698, 'subzone_code': 'FR-MAR'},
                    {'name': 'Toulouse', 'lat': 43.6047, 'lon': 1.4442, 'subzone_code': 'FR-TOU'}
                ]
            },
            'DE': {  # Germany
                'name': 'Germany',
                'subzones': [
                    {'name': 'Berlin', 'lat': 52.5200, 'lon': 13.4050, 'subzone_code': 'DE-BER'},
                    {'name': 'Munich', 'lat': 48.1351, 'lon': 11.5820, 'subzone_code': 'DE-MUN'},
                    {'name': 'Hamburg', 'lat': 53.5511, 'lon': 9.9937, 'subzone_code': 'DE-HAM'},
                    {'name': 'Frankfurt', 'lat': 50.1109, 'lon': 8.6821, 'subzone_code': 'DE-FRA'}
                ]
            },
            'GR': {  # Greece
                'name': 'Greece',
                'subzones': [
                    {'name': 'Athens', 'lat': 37.9838, 'lon': 23.7275, 'subzone_code': 'GR-ATH'},
                    {'name': 'Thessaloniki', 'lat': 40.6401, 'lon': 22.9444, 'subzone_code': 'GR-THE'},
                    {'name': 'Patras', 'lat': 38.2466, 'lon': 21.7346, 'subzone_code': 'GR-PAT'}
                ]
            },
            'HU': {  # Hungary
                'name': 'Hungary',
                'subzones': [
                    {'name': 'Budapest', 'lat': 47.4979, 'lon': 19.0402, 'subzone_code': 'HU-BUD'},
                    {'name': 'Debrecen', 'lat': 47.5316, 'lon': 21.6273, 'subzone_code': 'HU-DEB'},
                    {'name': 'Szeged', 'lat': 46.2530, 'lon': 20.1414, 'subzone_code': 'HU-SZE'}
                ]
            },
            'IE': {  # Ireland
                'name': 'Ireland',
                'subzones': [
                    {'name': 'Dublin', 'lat': 53.3498, 'lon': -6.2603, 'subzone_code': 'IE-DUB'},
                    {'name': 'Cork', 'lat': 51.8985, 'lon': -8.4756, 'subzone_code': 'IE-COR'},
                    {'name': 'Galway', 'lat': 53.2707, 'lon': -9.0568, 'subzone_code': 'IE-GAL'}
                ]
            },
            'IT': {  # Italy
                'name': 'Italy',
                'subzones': [
                    {'name': 'Rome', 'lat': 41.9028, 'lon': 12.4964, 'subzone_code': 'IT-ROM'},
                    {'name': 'Milan', 'lat': 45.4642, 'lon': 9.1900, 'subzone_code': 'IT-MIL'},
                    {'name': 'Naples', 'lat': 40.8518, 'lon': 14.2681, 'subzone_code': 'IT-NAP'},
                    {'name': 'Turin', 'lat': 45.0703, 'lon': 7.6869, 'subzone_code': 'IT-TUR'}
                ]
            },
            'LV': {  # Latvia
                'name': 'Latvia',
                'subzones': [
                    {'name': 'Riga', 'lat': 56.9496, 'lon': 24.1052, 'subzone_code': 'LV-RIG'},
                    {'name': 'Daugavpils', 'lat': 55.8747, 'lon': 26.5059, 'subzone_code': 'LV-DAU'},
                    {'name': 'Liepaja', 'lat': 56.5053, 'lon': 21.0107, 'subzone_code': 'LV-LIE'}
                ]
            },
            'LT': {  # Lithuania
                'name': 'Lithuania',
                'subzones': [
                    {'name': 'Vilnius', 'lat': 54.6872, 'lon': 25.2797, 'subzone_code': 'LT-VIL'},
                    {'name': 'Kaunas', 'lat': 54.8985, 'lon': 23.9036, 'subzone_code': 'LT-KAU'},
                    {'name': 'Klaipeda', 'lat': 55.7172, 'lon': 21.1175, 'subzone_code': 'LT-KLA'}
                ]
            },
            'LU': {  # Luxembourg
                'name': 'Luxembourg',
                'subzones': [
                    {'name': 'Luxembourg City', 'lat': 49.6116, 'lon': 6.1319, 'subzone_code': 'LU-LUX'},
                    {'name': 'Esch-sur-Alzette', 'lat': 49.4958, 'lon': 5.9809, 'subzone_code': 'LU-ESC'},
                    {'name': 'Dudelange', 'lat': 49.4814, 'lon': 6.0837, 'subzone_code': 'LU-DUD'}
                ]
            },
            'MT': {  # Malta
                'name': 'Malta',
                'subzones': [
                    {'name': 'Valletta', 'lat': 35.8989, 'lon': 14.5146, 'subzone_code': 'MT-VAL'},
                    {'name': 'Sliema', 'lat': 35.9122, 'lon': 14.5019, 'subzone_code': 'MT-SLI'},
                    {'name': 'Birkirkara', 'lat': 35.8972, 'lon': 14.4611, 'subzone_code': 'MT-BIR'}
                ]
            },
            'NL': {  # Netherlands
                'name': 'Netherlands',
                'subzones': [
                    {'name': 'Amsterdam', 'lat': 52.3676, 'lon': 4.9041, 'subzone_code': 'NL-AMS'},
                    {'name': 'Rotterdam', 'lat': 51.9244, 'lon': 4.4777, 'subzone_code': 'NL-ROT'},
                    {'name': 'The Hague', 'lat': 52.0705, 'lon': 4.3007, 'subzone_code': 'NL-HAG'},
                    {'name': 'Utrecht', 'lat': 52.0907, 'lon': 5.1214, 'subzone_code': 'NL-UTR'}
                ]
            },
            'PL': {  # Poland
                'name': 'Poland',
                'subzones': [
                    {'name': 'Warsaw', 'lat': 52.2297, 'lon': 21.0122, 'subzone_code': 'PL-WAW'},
                    {'name': 'Krakow', 'lat': 50.0647, 'lon': 19.9450, 'subzone_code': 'PL-KRK'},
                    {'name': 'Gdansk', 'lat': 54.3520, 'lon': 18.6466, 'subzone_code': 'PL-GDN'},
                    {'name': 'Wroclaw', 'lat': 51.1079, 'lon': 17.0385, 'subzone_code': 'PL-WRO'}
                ]
            },
            'PT': {  # Portugal
                'name': 'Portugal',
                'subzones': [
                    {'name': 'Lisbon', 'lat': 38.7223, 'lon': -9.1393, 'subzone_code': 'PT-LIS'},
                    {'name': 'Porto', 'lat': 41.1579, 'lon': -8.6291, 'subzone_code': 'PT-POR'},
                    {'name': 'Braga', 'lat': 41.5454, 'lon': -8.4265, 'subzone_code': 'PT-BRA'}
                ]
            },
            'RO': {  # Romania
                'name': 'Romania',
                'subzones': [
                    {'name': 'Bucharest', 'lat': 44.4268, 'lon': 26.1025, 'subzone_code': 'RO-BUC'},
                    {'name': 'Cluj-Napoca', 'lat': 46.7712, 'lon': 23.6236, 'subzone_code': 'RO-CLU'},
                    {'name': 'Timisoara', 'lat': 45.7489, 'lon': 21.2087, 'subzone_code': 'RO-TIM'},
                    {'name': 'Constanta', 'lat': 44.1598, 'lon': 28.6348, 'subzone_code': 'RO-CON'}
                ]
            },
            'SK': {  # Slovakia
                'name': 'Slovakia',
                'subzones': [
                    {'name': 'Bratislava', 'lat': 48.1486, 'lon': 17.1077, 'subzone_code': 'SK-BRA'},
                    {'name': 'Kosice', 'lat': 48.7164, 'lon': 21.2611, 'subzone_code': 'SK-KOS'},
                    {'name': 'Zilina', 'lat': 49.2231, 'lon': 18.7393, 'subzone_code': 'SK-ZIL'}
                ]
            },
            'SI': {  # Slovenia
                'name': 'Slovenia',
                'subzones': [
                    {'name': 'Ljubljana', 'lat': 46.0569, 'lon': 14.5058, 'subzone_code': 'SI-LJU'},
                    {'name': 'Maribor', 'lat': 46.5547, 'lon': 15.6459, 'subzone_code': 'SI-MAR'},
                    {'name': 'Celje', 'lat': 46.2311, 'lon': 15.2683, 'subzone_code': 'SI-CEL'}
                ]
            },
            'ES': {  # Spain
                'name': 'Spain',
                'subzones': [
                    {'name': 'Madrid', 'lat': 40.4168, 'lon': -3.7038, 'subzone_code': 'ES-MAD'},
                    {'name': 'Barcelona', 'lat': 41.3851, 'lon': 2.1734, 'subzone_code': 'ES-BCN'},
                    {'name': 'Seville', 'lat': 37.3891, 'lon': -5.9845, 'subzone_code': 'ES-SEV'},
                    {'name': 'Valencia', 'lat': 39.4699, 'lon': -0.3763, 'subzone_code': 'ES-VAL'}
                ]
            },
            'SE': {  # Sweden
                'name': 'Sweden',
                'subzones': [
                    {'name': 'Stockholm', 'lat': 59.3293, 'lon': 18.0686, 'subzone_code': 'SE-STO'},
                    {'name': 'Gothenburg', 'lat': 57.7089, 'lon': 11.9746, 'subzone_code': 'SE-GOT'},
                    {'name': 'Malmo', 'lat': 55.6050, 'lon': 13.0038, 'subzone_code': 'SE-MAL'},
                    {'name': 'Uppsala', 'lat': 59.8586, 'lon': 17.6389, 'subzone_code': 'SE-UPP'}
                ]
            }
        }
        
        # Parametry pogodowe do pobrania
        self.hourly_params = [
            'temperature_2m',
            'relativehumidity_2m', 
            'precipitation',
            'windspeed_10m',
            'winddirection_10m',
            'pressure_msl',
            'cloudcover'
        ]
        
        self.daily_params = [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'windspeed_10m_max',
            'winddirection_10m_dominant',
            'et0_fao_evapotranspiration'  # Pomocne przy obliczaniu HDD/CDD
        ]
    
    def _make_request(self, url: str, params: Dict, max_retries: int = 3) -> Optional[Dict]:
        """
        Wykonanie zapytania do API z retry logic
        
        Args:
            url: URL endpoint
            params: Parametry zapytania
            max_retries: Maksymalna liczba prób
            
        Returns:
            Odpowiedź JSON jako dict lub None w przypadku błędu
        """
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
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
    
    def get_historical_weather(self, latitude: float, longitude: float, 
                             start_date: datetime, end_date: datetime,
                             subzone_code: str, subzone_name: str) -> pd.DataFrame:
        """
        Pobieranie historycznych danych pogodowych dla lokalizacji
        
        Args:
            latitude: Szerokość geograficzna
            longitude: Długość geograficzna
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            subzone_code: Kod podstrefy (miasta)
            subzone_name: Nazwa podstrefy (miasta)
            
        Returns:
            DataFrame z danymi pogodowymi
        """
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'hourly': ','.join(self.hourly_params),
            'timezone': 'auto'
        }
        
        self.logger.info(f"Fetching historical weather for {subzone_name} ({subzone_code})")
        
        response_data = self._make_request(self.base_url, params)
        if not response_data:
            self.logger.warning(f"No weather data received for {subzone_name}")
            return pd.DataFrame()
        
        try:
            # Parsowanie danych godzinowych
            hourly_data = response_data.get('hourly', {})
            
            # Tworzenie DataFrame
            df_data = {
                'timestamp': pd.to_datetime(hourly_data.get('time', [])),
                'latitude': latitude,
                'longitude': longitude,
                'subzone_code': subzone_code,
                'subzone_name': subzone_name
            }
            
            # Mapowanie parametrów pogodowych
            param_mapping = {
                'temperature_2m': 'temperature_avg',
                'relativehumidity_2m': 'humidity',
                'precipitation': 'precipitation',
                'windspeed_10m': 'wind_speed',
                'winddirection_10m': 'wind_direction',
                'pressure_msl': 'air_pressure',
                'cloudcover': 'cloud_cover'
            }
            
            for api_param, db_column in param_mapping.items():
                df_data[db_column] = hourly_data.get(api_param, [])
            
            df = pd.DataFrame(df_data)
            
            # Obliczanie dodatkowych metryk
            df = self._calculate_derived_metrics(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing weather data for {subzone_name}: {str(e)}")
            return pd.DataFrame()
    
    def get_climate_indices(self, latitude: float, longitude: float,
                          start_date: datetime, end_date: datetime,
                          subzone_code: str, subzone_name: str) -> pd.DataFrame:
        """
        Pobieranie wskaźników klimatycznych (HDD/CDD)
        
        Args:
            latitude: Szerokość geograficzna
            longitude: Długość geograficzna
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            subzone_code: Kod podstrefy (miasta)
            subzone_name: Nazwa podstrefy (miasta)
            
        Returns:
            DataFrame ze wskaźnikami klimatycznymi
        """
        self.logger.info(f"Fetching climate indices for {subzone_name} ({subzone_code})")
        
        # Próbujemy bezpośrednio pobrać dane o heating/cooling degree days
        # Najpierw sprawdźmy czy API weather_forecast obsługuje te parametry
        try:
            # Najpierw próbujemy pobrać dane temperaturowe
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean',
                'timezone': 'auto'
            }
            
            response_data = self._make_request(self.climate_url, params)
            
            if not response_data or 'daily' not in response_data:
                # Jeśli climate_url nie działa, próbujemy z podstawowym URL
                params = {
                    'latitude': latitude,
                    'longitude': longitude,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean',
                    'timezone': 'auto'
                }
                response_data = self._make_request(self.base_url, params)
            
            if not response_data or 'daily' not in response_data:
                self.logger.warning(f"Failed to retrieve climate data for {subzone_name}")
                return pd.DataFrame()
                
            daily_data = response_data.get('daily', {})
            
            # Tworzenie DataFrame
            df = pd.DataFrame({
                'date': pd.to_datetime(daily_data.get('time', [])),
                'temperature_max': daily_data.get('temperature_2m_max', []),
                'temperature_min': daily_data.get('temperature_2m_min', []),
                'temperature_mean': daily_data.get('temperature_2m_mean', []),
                'latitude': latitude,
                'longitude': longitude,
                'subzone_code': subzone_code,
                'subzone_name': subzone_name
            })
            
            # Obliczamy heating_degree_days i cooling_degree_days na podstawie temperatury
            base_temp = 18.0  # Temperatura bazowa (w °C)
            
            # HDD: Suma wartości poniżej temperatury bazowej
            df['heating_degree_days'] = df['temperature_mean'].apply(
                lambda x: max(base_temp - x, 0)
            )
            
            # CDD: Suma wartości powyżej temperatury bazowej
            df['cooling_degree_days'] = df['temperature_mean'].apply(
                lambda x: max(x - base_temp, 0)
            )
            
            self.logger.info(f"Calculated climate indices for {subzone_name}: HDD and CDD from temperature data")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing climate data for {subzone_name}: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Obliczanie dodatkowych metryk pogodowych
        
        Args:
            df: DataFrame z podstawowymi danymi pogodowymi
            
        Returns:
            DataFrame z dodatkowymi metrykami
        """
        if df.empty:
            return df
        
        # Określanie warunków pogodowych na podstawie parametrów
        conditions = []
        for _, row in df.iterrows():
            condition = self._determine_weather_condition(
                row.get('precipitation', 0),
                row.get('cloud_cover', 0),
                row.get('wind_speed', 0),
                row.get('temperature_avg', 0)
            )
            conditions.append(condition)
        
        df['weather_condition'] = conditions
        
        # Obliczanie temperatury minimalnej i maksymalnej (dziennie)
        df['date'] = df['timestamp'].dt.date
        daily_temps = df.groupby(['date', 'subzone_code'])['temperature_avg'].agg(['min', 'max']).reset_index()
        daily_temps.columns = ['date', 'subzone_code', 'temperature_min', 'temperature_max']
        
        # Merge z danymi godzinowymi
        df = df.merge(daily_temps, on=['date', 'subzone_code'], how='left')
        
        # Obliczanie promieniowania słonecznego na podstawie zachmurzenia
        df['solar_radiation'] = (100 - df['cloud_cover']) / 100 * 1000  # Uproszczone
        
        return df
    
    def _determine_weather_condition(self, precipitation: float, cloud_cover: float,
                                   wind_speed: float, temperature: float) -> str:
        """
        Określanie typu warunków pogodowych
        
        Args:
            precipitation: Opady [mm]
            cloud_cover: Zachmurzenie [%]
            wind_speed: Prędkość wiatru [km/h]
            temperature: Temperatura [°C]
            
        Returns:
            Typ warunków pogodowych
        """
        # Sprawdzanie opadów
        if precipitation > 5:
            if temperature < 0:
                return 'Snow'
            else:
                return 'Rain'
        
        # Sprawdzanie wiatru
        if wind_speed > 50:
            return 'Windy'
        
        # Sprawdzanie zachmurzenia
        if cloud_cover < 20:
            return 'Clear'
        elif cloud_cover < 50:
            return 'Partly Cloudy'
        elif cloud_cover < 80:
            return 'Cloudy'
        else:
            return 'Overcast'
    
    def extract_all_weather_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Ekstrakcja danych pogodowych dla wszystkich stref i podstref UE
        
        Args:
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            DataFrame z danymi pogodowymi dla wszystkich stref i podstref
        """
        self.logger.info(f"Starting weather data extraction for EU countries from {start_date} to {end_date}")
        
        # Logowanie rozpoczęcia procesu
        try:
            self._log_process('WEATHER_EXTRACTION_EU', 'RUNNING')
        except Exception as e:
            self.logger.warning(f"Error logging process: {str(e)}")
        
        all_subzone_data = []
        total_countries = len(self.weather_locations)
        processed_countries = 0
        total_cities = sum(len(zone_info['subzones']) for zone_info in self.weather_locations.values())
        processed_cities = 0
        
        self.logger.info(f"Processing {total_countries} EU countries with {total_cities} cities total")
        
        try:
            for country_code, zone_info in self.weather_locations.items():
                self.logger.info(f"Processing weather for {zone_info['name']} ({country_code}) - {len(zone_info['subzones'])} cities")
                
                # Pobieranie danych dla każdej podstrefy (miasta) w strefie
                for subzone in zone_info['subzones']:
                    processed_cities += 1
                    self.logger.info(f"Fetching weather for {subzone['name']} ({subzone['subzone_code']}) - city {processed_cities}/{total_cities}")
                    
                    # Dane pogodowe
                    weather_df = self.get_historical_weather(
                        subzone['lat'], subzone['lon'], 
                        start_date, end_date,
                        subzone['subzone_code'], subzone['name']
                    )
                    
                    if not weather_df.empty:
                        # Dodanie informacji o strefie
                        weather_df['country_code'] = country_code
                        weather_df['zone_name'] = zone_info['name']
                        all_subzone_data.append(weather_df)
                        self.logger.info(f"Successfully extracted {len(weather_df)} records for {subzone['name']}")
                    else:
                        self.logger.warning(f"No data retrieved for {subzone['name']}")
                    
                    # Przerwa między zapytaniami aby nie przekroczyć limitów API
                    time.sleep(1.0)  # Zwiększona przerwa dla większej liczby miast
                
                processed_countries += 1
                self.logger.info(f"Completed {zone_info['name']} - processed {processed_countries}/{total_countries} countries")
                
                # Dodatkowa przerwa między krajami
                time.sleep(2.0)
            
            # Łączenie wszystkich danych
            if all_subzone_data:
                final_df = pd.concat(all_subzone_data, ignore_index=True)
                total_records = len(final_df)
                
                self.logger.info(f"Successfully extracted weather data for all EU countries: {total_records} total records")
                
                # Logowanie sukcesu
                try:
                    self._log_process('WEATHER_EXTRACTION_EU', 'SUCCESS', total_records)
                except Exception as e:
                    self.logger.warning(f"Error logging process: {str(e)}")
                
                return final_df
            else:
                self.logger.warning("No weather data extracted for any EU location")
                try:
                    self._log_process('WEATHER_EXTRACTION_EU', 'SUCCESS', 0)
                except Exception as e:
                    self.logger.warning(f"Error logging process: {str(e)}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error during EU weather data extraction: {str(e)}")
            try:
                self._log_process('WEATHER_EXTRACTION_EU', 'FAILED', 0, str(e))
            except Exception as log_e:
                self.logger.warning(f"Error logging process: {str(log_e)}")
            raise
    
    def extract_climate_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Ekstrakcja wskaźników klimatycznych dla wszystkich stref i podstref UE
        
        Args:
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            DataFrame ze wskaźnikami klimatycznymi
        """
        self.logger.info(f"Starting climate data extraction for EU countries from {start_date} to {end_date}")
        
        try:
            self._log_process('CLIMATE_EXTRACTION_EU', 'RUNNING')
        except Exception as e:
            self.logger.warning(f"Error logging climate process: {str(e)}")
        
        all_climate_data = []
        total_countries = len(self.weather_locations)
        processed_countries = 0
        total_cities = sum(len(zone_info['subzones']) for zone_info in self.weather_locations.values())
        processed_cities = 0
        
        self.logger.info(f"Processing climate data for {total_countries} EU countries with {total_cities} cities total")
        
        try:
            for country_code, zone_info in self.weather_locations.items():
                self.logger.info(f"Processing climate data for {zone_info['name']} ({country_code}) - {len(zone_info['subzones'])} cities")
                
                # Pobieranie danych dla każdej podstrefy (miasta)
                for subzone in zone_info['subzones']:
                    processed_cities += 1
                    self.logger.info(f"Fetching climate data for {subzone['name']} ({subzone['subzone_code']}) - city {processed_cities}/{total_cities}")
                    
                    climate_df = self.get_climate_indices(
                        subzone['lat'], subzone['lon'], 
                        start_date, end_date,
                        subzone['subzone_code'], subzone['name']
                    )
                    
                    if not climate_df.empty:
                        # Dodanie informacji o strefie
                        climate_df['country_code'] = country_code
                        climate_df['zone_name'] = zone_info['name']
                        all_climate_data.append(climate_df)
                        self.logger.info(f"Successfully extracted {len(climate_df)} climate records for {subzone['name']}")
                    else:
                        self.logger.warning(f"No climate data retrieved for {subzone['name']}")
                    
                    time.sleep(1.0)
                
                processed_countries += 1
                self.logger.info(f"Completed climate data for {zone_info['name']} - processed {processed_countries}/{total_countries} countries")
                
                # Dodatkowa przerwa między krajami
                time.sleep(2.0)
            
            # Łączenie wszystkich danych
            if all_climate_data:
                final_climate_df = pd.concat(all_climate_data, ignore_index=True)
                total_records = len(final_climate_df)
                
                self.logger.info(f"Successfully extracted climate data for all EU countries: {total_records} total records")
                
                try:
                    self._log_process('CLIMATE_EXTRACTION_EU', 'SUCCESS', total_records)
                except Exception as e:
                    self.logger.warning(f"Error logging climate process: {str(e)}")
                
                return final_climate_df
            else:
                self.logger.warning("No climate data extracted for any EU location")
                try:
                    self._log_process('CLIMATE_EXTRACTION_EU', 'SUCCESS', 0)
                except Exception as e:
                    self.logger.warning(f"Error logging climate process: {str(e)}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error during EU climate data extraction: {str(e)}")
            try:
                self._log_process('CLIMATE_EXTRACTION_EU', 'FAILED', 0, str(e))
            except Exception as log_e:
                self.logger.warning(f"Error logging climate process: {str(log_e)}")
            raise
    
    def save_to_staging(self, weather_data: pd.DataFrame, climate_data: pd.DataFrame = None) -> bool:
        """
        Zapisanie danych pogodowych do tabel staging
        
        Args:
            weather_data: DataFrame z danymi pogodowymi
            climate_data: DataFrame ze wskaźnikami klimatycznymi (opcjonalnie)
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        try:
            # Sprawdzamy czy connection_string ma odpowiedni format
            if 'Driver' not in self.connection_string and 'DSN' not in self.connection_string:
                self.logger.warning("Connection string may be invalid, trying to fix it")
                # Próba naprawy connection string
                self.connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
                self.logger.info(f"Using connection string: {self.connection_string}")
            
            conn = pyodbc.connect(self.connection_string)
            self.logger.info("Connected to database successfully")
            
            # Najpierw zapisujemy dane do CSV jako bezpieczną kopię, niezależnie od wyniku
            try:
                if not weather_data.empty:
                    csv_filename = f'staging_weather_data_eu_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    weather_data.to_csv(csv_filename, index=False)
                    self.logger.info(f"Weather data saved to {csv_filename} as backup")
                
                if climate_data is not None and not climate_data.empty:
                    csv_filename = f'staging_climate_data_eu_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    climate_data.to_csv(csv_filename, index=False)
                    self.logger.info(f"Climate data saved to {csv_filename} as backup")
            except Exception as csv_e:
                self.logger.error(f"Error saving to CSV backup: {str(csv_e)}")
            
            # Zapis danych pogodowych
            if not weather_data.empty:
                self.logger.info("Creating or updating weather staging table")
                self._create_weather_staging_table(conn)
                
                cursor = conn.cursor()
                try:
                    cursor.execute("IF OBJECT_ID('staging_weather_data', 'U') IS NOT NULL TRUNCATE TABLE staging_weather_data")
                    self.logger.info("Truncated staging_weather_data table")
                except Exception as e:
                    self.logger.warning(f"Could not truncate table: {str(e)}")
                    
                self._bulk_insert_weather_data(conn, weather_data)
                self.logger.info(f"Saved {len(weather_data)} weather records to staging")
            
            # Zapis wskaźników klimatycznych
            if climate_data is not None and not climate_data.empty:
                self.logger.info("Creating or updating climate staging table")
                self._create_climate_staging_table(conn)
                
                cursor = conn.cursor()
                try:
                    cursor.execute("IF OBJECT_ID('staging_climate_data', 'U') IS NOT NULL TRUNCATE TABLE staging_climate_data")
                    self.logger.info("Truncated staging_climate_data table")
                except Exception as e:
                    self.logger.warning(f"Could not truncate table: {str(e)}")
                
                self._bulk_insert_climate_data(conn, climate_data)
                self.logger.info(f"Saved {len(climate_data)} climate records to staging")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data to staging: {str(e)}")
            
            # Dane już zapisane do CSV jako bezpieczna kopia
            return False
    
    def _create_weather_staging_table(self, conn):
        """Utworzenie tabeli staging dla danych pogodowych"""
        cursor = conn.cursor()
        
        # Sprawdź czy tabela istnieje
        cursor.execute("IF OBJECT_ID('staging_weather_data', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            # Jeśli tabela nie istnieje, stwórz ją
            create_sql = """
            CREATE TABLE staging_weather_data (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                timestamp DATETIME2 NOT NULL,
                country_code NVARCHAR(5) NOT NULL,
                zone_name NVARCHAR(100) NOT NULL,
                subzone_code NVARCHAR(10) NOT NULL,
                subzone_name NVARCHAR(100) NOT NULL,
                temperature_avg DECIMAL(5,2),
                temperature_min DECIMAL(5,2),
                temperature_max DECIMAL(5,2),
                humidity DECIMAL(5,2),
                precipitation DECIMAL(5,2),
                wind_speed DECIMAL(5,2),
                wind_direction INT,
                air_pressure DECIMAL(7,2),
                cloud_cover DECIMAL(5,2),
                solar_radiation DECIMAL(7,2),
                weather_condition NVARCHAR(30),
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                created_at DATETIME2 DEFAULT GETDATE()
            )
            """
            
            cursor.execute(create_sql)
            self.logger.info("Created new staging_weather_data table")
        else:
            # Tabela istnieje, sprawdź czy ma wymagane kolumny i dodaj jeśli nie
            # Dla każdej potencjalnie brakującej kolumny dodaj ALTER TABLE
            alter_statements = [
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'subzone_code' AND Object_ID = Object_ID('staging_weather_data')) ALTER TABLE staging_weather_data ADD subzone_code NVARCHAR(10) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'subzone_name' AND Object_ID = Object_ID('staging_weather_data')) ALTER TABLE staging_weather_data ADD subzone_name NVARCHAR(100) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'latitude' AND Object_ID = Object_ID('staging_weather_data')) ALTER TABLE staging_weather_data ADD latitude DECIMAL(10,6) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'longitude' AND Object_ID = Object_ID('staging_weather_data')) ALTER TABLE staging_weather_data ADD longitude DECIMAL(10,6) NULL"
            ]
            
            for statement in alter_statements:
                try:
                    cursor.execute(statement)
                    self.logger.info(f"Altered table with: {statement}")
                except Exception as e:
                    self.logger.warning(f"Failed to execute: {statement}, Error: {str(e)}")
        
        conn.commit()
    
    def _create_climate_staging_table(self, conn):
        """Utworzenie tabeli staging dla wskaźników klimatycznych"""
        cursor = conn.cursor()
        
        # Sprawdź czy tabela istnieje
        cursor.execute("IF OBJECT_ID('staging_climate_data', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            # Jeśli tabela nie istnieje, stwórz ją
            create_sql = """
            CREATE TABLE staging_climate_data (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                date DATE NOT NULL,
                country_code NVARCHAR(5) NOT NULL,
                zone_name NVARCHAR(100) NOT NULL,
                subzone_code NVARCHAR(10) NOT NULL,
                subzone_name NVARCHAR(100) NOT NULL,
                heating_degree_days DECIMAL(5,2),
                cooling_degree_days DECIMAL(5,2),
                temperature_max DECIMAL(5,2),
                temperature_min DECIMAL(5,2),
                temperature_mean DECIMAL(5,2),
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                created_at DATETIME2 DEFAULT GETDATE()
            )
            """
            
            cursor.execute(create_sql)
            self.logger.info("Created new staging_climate_data table")
        else:
            # Tabela istnieje, sprawdź czy ma wymagane kolumny i dodaj jeśli nie
            alter_statements = [
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'subzone_code' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD subzone_code NVARCHAR(10) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'subzone_name' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD subzone_name NVARCHAR(100) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'temperature_max' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD temperature_max DECIMAL(5,2) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'temperature_min' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD temperature_min DECIMAL(5,2) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'temperature_mean' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD temperature_mean DECIMAL(5,2) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'latitude' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD latitude DECIMAL(10,6) NULL",
                "IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = 'longitude' AND Object_ID = Object_ID('staging_climate_data')) ALTER TABLE staging_climate_data ADD longitude DECIMAL(10,6) NULL"
            ]
            
            for statement in alter_statements:
                try:
                    cursor.execute(statement)
                    self.logger.info(f"Altered table with: {statement}")
                except Exception as e:
                    self.logger.warning(f"Failed to execute: {statement}, Error: {str(e)}")
        
        conn.commit()
    
    def _bulk_insert_weather_data(self, conn, df: pd.DataFrame):
        """Bulk insert danych pogodowych"""
        cursor = conn.cursor()
        
        # Sprawdź jakie kolumny istnieją w tabeli
        try:
            cursor.execute("SELECT TOP 0 * FROM staging_weather_data")
            table_columns = [column[0] for column in cursor.description]
            self.logger.info(f"Found existing columns in staging_weather_data: {table_columns}")
        except Exception as e:
            self.logger.error(f"Error reading table structure: {str(e)}")
            # W przypadku błędu, załóżmy standardowy zestaw kolumn
            table_columns = [
                'timestamp', 'country_code', 'zone_name', 'temperature_avg',
                'temperature_min', 'temperature_max', 'humidity', 'precipitation',
                'wind_speed', 'wind_direction', 'air_pressure', 'cloud_cover',
                'solar_radiation', 'weather_condition'
            ]
        
        # Wybierz tylko te kolumny z DataFrame, które istnieją w tabeli
        df_columns = df.columns.tolist()
        existing_columns = [col for col in df_columns if col in table_columns]
        
        # Jeśli brakuje nowych kolumn (subzone_code, subzone_name itd.) uzupełnij NULL-ami
        if len(existing_columns) < len(table_columns):
            for column in table_columns:
                if column not in existing_columns and column not in ['id', 'created_at']:
                    # Wypełniamy wartości dla brakujących kolumn
                    if column == 'subzone_code':
                        df['subzone_code'] = 'UNKNOWN'
                    elif column == 'subzone_name':
                        df['subzone_name'] = 'Unknown'
                    elif column == 'latitude' or column == 'longitude':
                        df[column] = 0.0
                    
                    # Dodaj nową kolumnę do listy istniejących
                    existing_columns.append(column)
                    
        self.logger.info(f"Using columns for insert: {existing_columns}")
        
        # Omijamy kolumny id i created_at, które są generowane przez bazę danych
        insert_columns = [col for col in existing_columns if col not in ['id', 'created_at']]
        
        for _, row in df.iterrows():
            values = []
            for col in insert_columns:
                value = row.get(col, None)
                if pd.isna(value):
                    values.append(None)
                else:
                    values.append(value)
            
            placeholders = ', '.join(['?' for _ in values])
            insert_sql = f"""
                INSERT INTO staging_weather_data 
                ({', '.join(insert_columns)}) 
                VALUES ({placeholders})
            """
            
            cursor.execute(insert_sql, values)
        
        # Commit po wszystkich wierszach
        conn.commit()
    
    def _bulk_insert_climate_data(self, conn, df: pd.DataFrame):
        """Bulk insert wskaźników klimatycznych"""
        cursor = conn.cursor()
        
        # Sprawdź jakie kolumny istnieją w tabeli
        try:
            cursor.execute("SELECT TOP 0 * FROM staging_climate_data")
            table_columns = [column[0] for column in cursor.description]
            self.logger.info(f"Found existing columns in staging_climate_data: {table_columns}")
        except Exception as e:
            self.logger.error(f"Error reading climate table structure: {str(e)}")
            # W przypadku błędu, załóżmy standardowy zestaw kolumn
            table_columns = [
                'date', 'country_code', 'zone_name', 'heating_degree_days', 'cooling_degree_days'
            ]
        
        # Wybierz tylko te kolumny z DataFrame, które istnieją w tabeli
        df_columns = df.columns.tolist()
        existing_columns = [col for col in df_columns if col in table_columns]
        
        # Jeśli brakuje nowych kolumn uzupełnij NULL-ami
        if len(existing_columns) < len(table_columns):
            for column in table_columns:
                if column not in existing_columns and column not in ['id', 'created_at']:
                    # Wypełniamy wartości dla brakujących kolumn
                    if column == 'subzone_code':
                        df['subzone_code'] = 'UNKNOWN'
                    elif column == 'subzone_name':
                        df['subzone_name'] = 'Unknown'
                    elif column in ['temperature_max', 'temperature_min', 'temperature_mean', 'latitude', 'longitude']:
                        df[column] = 0.0
                    
                    # Dodaj nową kolumnę do listy istniejących
                    existing_columns.append(column)
        
        self.logger.info(f"Using columns for climate insert: {existing_columns}")
        
        # Omijamy kolumny id i created_at, które są generowane przez bazę danych
        insert_columns = [col for col in existing_columns if col not in ['id', 'created_at']]
        
        for _, row in df.iterrows():
            values = []
            for col in insert_columns:
                value = row.get(col, None)
                if pd.isna(value):
                    values.append(None)
                else:
                    values.append(value)
            
            placeholders = ', '.join(['?' for _ in values])
            insert_sql = f"""
                INSERT INTO staging_climate_data 
                ({', '.join(insert_columns)}) 
                VALUES ({placeholders})
            """
            
            cursor.execute(insert_sql, values)
        
        # Commit po wszystkich wierszach
        conn.commit()
    
    def _log_process(self, process_name: str, status: str, records: int = 0, error_msg: str = None):
        """Logowanie procesu do bazy danych"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Najpierw sprawdź czy procedura istnieje
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

# Funkcja główna do uruchomienia z SSIS
def main():
    """Główna funkcja wywoływana przez SSIS"""
    import sys
    import os
    from datetime import datetime, timedelta
    
    # Parametry z SSIS lub zmiennych środowiskowych
    connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Sprawdzenie czy connection_string ma odpowiedni format
    if not connection_string or ('Driver' not in connection_string and 'DSN' not in connection_string):
        # Domyślny connection string jako bezpieczna opcja
        connection_string = "Driver={SQL Server};Server=localhost;Database=EnergyWeatherDW;Trusted_Connection=yes;"
        print(f"Using default connection string: {connection_string}")
    
    # Domyślnie pobieramy dane z ostatnich 36 godzin
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(hours=36)
    
    # Parametry z argumentów linii komend (opcjonalnie)
    if len(sys.argv) > 1:
        try:
            start_date_str = sys.argv[1]
            if start_date_str.startswith('"') and start_date_str.endswith('"'):
                start_date_str = start_date_str[1:-1]
            start_date = datetime.fromisoformat(start_date_str)
            print(f"Using start_date from arguments: {start_date}")
        except Exception as e:
            print(f"Error parsing start_date: {e}, using default: {start_date}")
    
    if len(sys.argv) > 2:
        try:
            end_date_str = sys.argv[2]
            if end_date_str.startswith('"') and end_date_str.endswith('"'):
                end_date_str = end_date_str[1:-1]
            end_date = datetime.fromisoformat(end_date_str)
            print(f"Using end_date from arguments: {end_date}")
        except Exception as e:
            print(f"Error parsing end_date: {e}, using default: {end_date}")
    
    print(f"Data extraction period: {start_date} to {end_date}")
    print("Starting EU weather data extraction for all 27 EU countries...")
    
    try:
        # Inicjalizacja klienta
        client = OpenMeteoClient(connection_string)
        
        # Wyświetl informacje o krajach i miastach do przetworzenia
        total_countries = len(client.weather_locations)
        total_cities = sum(len(zone_info['subzones']) for zone_info in client.weather_locations.values())
        print(f"Processing {total_countries} EU countries with {total_cities} cities:")
        
        for country_code, zone_info in client.weather_locations.items():
            cities = [subzone['name'] for subzone in zone_info['subzones']]
            print(f"  {zone_info['name']} ({country_code}): {', '.join(cities)}")
        
        # Ekstrakcja danych pogodowych
        print("\nStarting weather data extraction...")
        weather_data = client.extract_all_weather_data(start_date, end_date)
        
        if weather_data.empty:
            print("Warning: No weather data extracted")
        else:
            print(f"Successfully extracted {len(weather_data)} weather records for all EU subzones")
            
            # Podsumowanie danych per kraj
            country_summary = weather_data.groupby('country_code').size().reset_index(name='records')
            print("\nWeather data summary by country:")
            for _, row in country_summary.iterrows():
                country_name = client.weather_locations[row['country_code']]['name']
                print(f"  {country_name} ({row['country_code']}): {row['records']} records")
        
        # Ekstrakcja wskaźników klimatycznych (tylko dla pełnych dni)
        climate_start = start_date.replace(hour=0, minute=0, second=0)
        climate_end = end_date.replace(hour=0, minute=0, second=0)
        print(f"\nStarting climate data extraction from {climate_start} to {climate_end}...")
        climate_data = client.extract_climate_data(climate_start, climate_end)
        
        if climate_data.empty:
            print("Warning: No climate data extracted")
        else:
            print(f"Successfully extracted {len(climate_data)} climate records for all EU subzones")
            
            # Podsumowanie danych klimatycznych per kraj
            climate_summary = climate_data.groupby('country_code').size().reset_index(name='records')
            print("\nClimate data summary by country:")
            for _, row in climate_summary.iterrows():
                country_name = client.weather_locations[row['country_code']]['name']
                print(f"  {country_name} ({row['country_code']}): {row['records']} records")
        
        # Zapis do staging
        print("\nSaving data to staging tables...")
        success = client.save_to_staging(weather_data, climate_data)
        
        if success:
            print("EU weather and climate data extraction completed successfully")
            print(f"Total weather records: {len(weather_data) if not weather_data.empty else 0}")
            print(f"Total climate records: {len(climate_data) if not climate_data.empty else 0}")
            sys.exit(0)
        else:
            print("Error occurred during EU weather data extraction")
            sys.exit(1)
            
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()