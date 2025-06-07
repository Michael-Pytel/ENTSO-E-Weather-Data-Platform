"""
DimensionProcessor.py
Przetwarzanie danych dla tabel wymiarowych
Generowanie wymiarów daty, czasu, mapowanie kodów stref, itp.
"""

import pandas as pd
from datetime import datetime, timedelta, date
import pyodbc
import logging
from typing import Dict, List, Optional, Tuple
import calendar
# Usunięto import holidays - nie będziemy go używać w tej wersji

class DimensionProcessor:
    """Procesor do tworzenia i aktualizacji wymiarów hurtowni danych"""
    
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
    
    def process_bidding_zone_dimension(self, socioeconomic_data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Przetwarzanie wymiaru stref przetargowych z danymi socjoekonomicznymi
        
        Args:
            socioeconomic_data: DataFrame z danymi socjoekonomicznymi z Eurostat
            
        Returns:
            DataFrame z wymiarem stref przetargowych
        """
        self.logger.info("Processing bidding zone dimension")
        
        zones_list = []
        
        for zone_code, zone_info in self.bidding_zone_mapping.items():
            zone_record = {
                'bidding_zone_code': zone_code,
                'bidding_zone_name': zone_info['name'],
                'primary_country': zone_info['country'],
                'secondary_countries': 'None',
                'control_area': zone_info['control_area'],
                'timezone': zone_info['timezone'],
                'population': 0,
                'gdp_per_capita': 0.0,
                'energy_intensity': 0.0,
                'electricity_price_avg': 0.0
            }
            
            # Wzbogacenie danymi socjoekonomicznymi jeśli dostępne
            if socioeconomic_data is not None and not socioeconomic_data.empty:
                country_data = socioeconomic_data[
                    socioeconomic_data['country_code'] == zone_info['country']
                ]
                
                if not country_data.empty:
                    # Znajdź najnowsze dane dla danego kraju
                    latest_data = country_data.sort_values('year', ascending=False).iloc[0]
                    
                    zone_record.update({
                        'population': latest_data.get('population', 0) or 0,
                        'gdp_per_capita': latest_data.get('gdp_per_capita', 0.0) or 0.0,
                        'energy_intensity': latest_data.get('energy_intensity', 0.0) or 0.0,
                        'electricity_price_avg': latest_data.get('electricity_price_avg', 0.0) or 0.0
                    })
            
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
        
        # Mapowanie stref pogodowych na strefy przetargowe
        climate_mapping = {
            'PL': {'climate': 'Continental', 'elevation': 173, 'coastal': 'Partial', 'urban': 'Mixed'},
            'DE': {'climate': 'Temperate Oceanic', 'elevation': 263, 'coastal': 'Partial', 'urban': 'High'},
            'FR': {'climate': 'Temperate', 'elevation': 375, 'coastal': 'Partial', 'urban': 'High'},
            'ES': {'climate': 'Mediterranean', 'elevation': 660, 'coastal': 'Extensive', 'urban': 'Medium'},
            'IT': {'climate': 'Mediterranean', 'elevation': 538, 'coastal': 'Extensive', 'urban': 'Medium'}
        }
        
        # Pobierz ID stref przetargowych z ramki danych
        bidding_zone_id_map = {}
        if not bidding_zones.empty and 'bidding_zone_code' in bidding_zones.columns:
            for _, zone in bidding_zones.iterrows():
                bidding_zone_id_map[zone['bidding_zone_code']] = _+1  # ID bazuje na indeksie (numerowane od 1)
        
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
                'urbanization_level': climate_info['urban']
            }
            
            weather_zones_list.append(weather_zone_record)
        
        return pd.DataFrame(weather_zones_list)
    
    def generate_socioeconomic_profile_dimension(self, socioeconomic_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generowanie wymiaru profili socjoekonomicznych
        
        Args:
            socioeconomic_data: DataFrame z danymi socjoekonomicznymi
            
        Returns:
            DataFrame z wymiarem profili socjoekonomicznych
        """
        self.logger.info("Generating socioeconomic profile dimension")
        
        if socioeconomic_data is None or socioeconomic_data.empty:
            # Generowanie domyślnych profili
            return self._generate_default_socioeconomic_profiles()
        
        profiles_list = []
        
        # Pobierz ID stref przetargowych z bazy danych
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute("SELECT bidding_zone_id, bidding_zone_code FROM dim_bidding_zone")
            bidding_zone_ids = {row.bidding_zone_code: row.bidding_zone_id for row in cursor.fetchall()}
            conn.close()
        except Exception as e:
            self.logger.warning(f"Could not fetch bidding zone IDs from database: {str(e)}")
            # Tworzymy mapowanie bazując na indeksach w self.bidding_zone_mapping
            bidding_zone_ids = {}
            for i, (zone_code, _) in enumerate(self.bidding_zone_mapping.items(), 1):
                bidding_zone_ids[zone_code] = i
        
        for _, data in socioeconomic_data.iterrows():
            # Mapowanie strefy przetargowej na podstawie kodu kraju
            bidding_zone_code = None
            for zone_code, zone_info in self.bidding_zone_mapping.items():
                if zone_info['country'] == data['country_code']:
                    bidding_zone_code = zone_code
                    break
            
            if bidding_zone_code is None:
                continue  # Pomijamy kraje bez strefy przetargowej
            
            # Pobierz ID strefy przetargowej
            bidding_zone_id = bidding_zone_ids.get(bidding_zone_code, 0)
            if bidding_zone_id == 0:
                self.logger.warning(f"No bidding zone ID found for code {bidding_zone_code}")
                continue
            
            # Klasyfikacja profilu na podstawie wskaźników ekonomicznych
            profile_name = self._classify_socioeconomic_profile(data)
            
            profile_record = {
                'profile_name': profile_name,
                'bidding_zone_id': bidding_zone_id,  # Używamy ID zamiast kodu
                'avg_income_level': data.get('gdp_per_capita', 0.0) or 0.0,
                'unemployment_rate': data.get('unemployment_rate', 0.0) or 0.0,
                'urbanization_rate': self._estimate_urbanization_rate(data['country_code']),
                'service_sector_percentage': self._estimate_service_sector(data['country_code']),
                'industry_sector_percentage': self._estimate_industry_sector(data['country_code']),
                'energy_poverty_rate': self._estimate_energy_poverty(data.get('gdp_per_capita', 0)),
                'residential_percentage': 30.0,  # Szacunkowy udział sektora mieszkaniowego
                'commercial_percentage': 25.0,   # Szacunkowy udział sektora komercyjnego
                'industrial_percentage': 45.0,   # Szacunkowy udział sektora przemysłowego
                'avg_household_size': self._estimate_household_size(data['country_code']),
                'primary_heating_type': self._determine_primary_heating(data['country_code'])
            }
            
            profiles_list.append(profile_record)
        
        return pd.DataFrame(profiles_list)
    
    def _generate_default_socioeconomic_profiles(self) -> pd.DataFrame:
        """Generowanie domyślnych profili socjoekonomicznych"""
        default_profiles = []
        
        # Pobierz ID stref przetargowych z bazy danych
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute("SELECT bidding_zone_id, bidding_zone_code FROM dim_bidding_zone")
            bidding_zone_ids = {row.bidding_zone_code: row.bidding_zone_id for row in cursor.fetchall()}
            conn.close()
        except Exception as e:
            self.logger.warning(f"Could not fetch bidding zone IDs from database: {str(e)}")
            # Tworzymy mapowanie bazując na indeksach w self.bidding_zone_mapping
            bidding_zone_ids = {}
            for i, (zone_code, _) in enumerate(self.bidding_zone_mapping.items(), 1):
                bidding_zone_ids[zone_code] = i
        
        for zone_code, zone_info in self.bidding_zone_mapping.items():
            bidding_zone_id = bidding_zone_ids.get(zone_code, 0)
            if bidding_zone_id == 0:
                self.logger.warning(f"No bidding zone ID found for code {zone_code}")
                continue
                
            profile_record = {
                'profile_name': 'Standard',
                'bidding_zone_id': bidding_zone_id,  # Używamy ID zamiast kodu
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
                'primary_heating_type': self._determine_primary_heating(zone_info['country'])
            }
            
            default_profiles.append(profile_record)
        
        return pd.DataFrame(default_profiles)
    
    def _classify_socioeconomic_profile(self, data: pd.Series) -> str:
        """Klasyfikacja profilu socjoekonomicznego na podstawie wskaźników"""
        gdp_per_capita = data.get('gdp_per_capita', 0) or 0
        unemployment_rate = data.get('unemployment_rate', 0) or 0
        
        if gdp_per_capita > 40000:
            if unemployment_rate < 5:
                return 'High Income - Low Unemployment'
            else:
                return 'High Income - Moderate Unemployment'
        elif gdp_per_capita > 25000:
            if unemployment_rate < 8:
                return 'Middle Income - Low Unemployment'
            else:
                return 'Middle Income - High Unemployment'
        else:
            return 'Lower Middle Income'
    
    def _estimate_urbanization_rate(self, country_code: str) -> float:
        """Szacowanie poziomu urbanizacji na podstawie kodu kraju"""
        urbanization_rates = {
            'PL': 60.0, 'DE': 77.0, 'FR': 81.0, 'ES': 80.0, 'IT': 70.0,
            'CZ': 74.0, 'SK': 54.0, 'HU': 72.0
        }
        return urbanization_rates.get(country_code, 65.0)
    
    def _estimate_service_sector(self, country_code: str) -> float:
        """Szacowanie udziału sektora usług"""
        service_sectors = {
            'PL': 65.0, 'DE': 69.0, 'FR': 79.0, 'ES': 76.0, 'IT': 74.0,
            'CZ': 60.0, 'SK': 62.0, 'HU': 65.0
        }
        return service_sectors.get(country_code, 68.0)
    
    def _estimate_industry_sector(self, country_code: str) -> float:
        """Szacowanie udziału sektora przemysłowego"""
        industry_sectors = {
            'PL': 33.0, 'DE': 29.0, 'FR': 19.0, 'ES': 23.0, 'IT': 24.0,
            'CZ': 37.0, 'SK': 35.0, 'HU': 31.0
        }
        return industry_sectors.get(country_code, 30.0)
    
    def _estimate_energy_poverty(self, gdp_per_capita: float) -> float:
        """Szacowanie poziomu ubóstwa energetycznego na podstawie PKB per capita"""
        if gdp_per_capita > 40000:
            return 3.0
        elif gdp_per_capita > 25000:
            return 8.0
        elif gdp_per_capita > 15000:
            return 15.0
        else:
            return 25.0
    
    def _estimate_household_size(self, country_code: str) -> int:
        """Szacowanie średniej wielkości gospodarstwa domowego"""
        household_sizes = {
            'PL': 3, 'DE': 2, 'FR': 2, 'ES': 3, 'IT': 2,
            'CZ': 2, 'SK': 3, 'HU': 2
        }
        return household_sizes.get(country_code, 2)
    
    def _determine_primary_heating(self, country_code: str) -> str:
        """Określenie głównego typu ogrzewania"""
        heating_types = {
            'PL': 'District Heating', 'DE': 'Natural Gas', 'FR': 'Electricity',
            'ES': 'Natural Gas', 'IT': 'Natural Gas', 'CZ': 'District Heating',
            'SK': 'Natural Gas', 'HU': 'Natural Gas'
        }
        return heating_types.get(country_code, 'Natural Gas')
    
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
        """Utworzenie tabeli staging dla wymiaru"""
        cursor = conn.cursor()
        
        # Mapowanie typów kolumn
        column_definitions = []
        
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
                max_length = df[column].astype(str).str.len().max() if not df[column].empty else 50
                max_length = min(max(max_length, 10), 500)  # Ograniczenie 10-500 znaków
                col_type = f"NVARCHAR({max_length})"
            
            column_definitions.append(f"{column} {col_type}")
        
        column_definitions.append("created_at DATETIME2 DEFAULT GETDATE()")
        
        create_sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
        CREATE TABLE {table_name} (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            {', '.join(column_definitions)}
        )
        """
        
        cursor.execute(create_sql)
        conn.commit()
    
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
    start_date = date(today.year - 1, 1, 1)
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
    
    logger.info(f"Dimension processing period: {start_date} to {end_date}")
    
    try:
        # Inicjalizacja procesora wymiarów
        processor = DimensionProcessor(connection_string)
        
        # Pobranie danych socjoekonomicznych ze staging (jeśli dostępne)
        socioeconomic_data = None
        try:
            conn = pyodbc.connect(connection_string)
            # Sprawdź czy tabela istnieje przed próbą odczytu
            cursor = conn.cursor()
            cursor.execute("""
                IF OBJECT_ID('staging_eurostat_integrated', 'U') IS NOT NULL
                    SELECT 1
                ELSE
                    SELECT 0
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                socioeconomic_data = pd.read_sql(
                    "SELECT * FROM staging_eurostat_integrated",
                    conn
                )
                logger.info(f"Loaded {len(socioeconomic_data)} socioeconomic records from staging")
            else:
                logger.warning("staging_eurostat_integrated table does not exist")
                
            conn.close()
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
        success = processor.save_dimensions_to_staging(dimensions)
        
        if success:
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
    