"""
ENTSOEClient.py
Klient API do komunikacji z ENTSO-E Transparency Platform
Pobiera dane o zużyciu energii i generacji wg typów źródeł
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Tuple
import pyodbc
import os
import sys

class ENTSOEClient:
    """Klient API dla ENTSO-E Transparency Platform"""
    
    def __init__(self, security_token: str, connection_string: str):
        """
        Inicjalizacja klienta ENTSO-E
        
        Args:
            security_token: Token API dla ENTSO-E
            connection_string: String połączenia z bazą danych
        """
        self.security_token = security_token
        self.base_url = "https://web-api.tp.entsoe.eu/api"
        self.connection_string = connection_string
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Logowanie parametrów (bez wrażliwych danych)
        self.logger.info(f"Initializing ENTSOEClient with connection to {self.base_url}")
        
        # Mapowanie stref przetargowych
        self.bidding_zones = {
            '10YAT-APG------L': {'name': 'Austria', 'country': 'AT', 'timezone': 'Europe/Vienna'},
            '10YBE----------2': {'name': 'Belgium', 'country': 'BE', 'timezone': 'Europe/Brussels'},
            '10YBG-ESO------M': {'name': 'Bulgaria', 'country': 'BG', 'timezone': 'Europe/Sofia'},
            '10YHR-HEP------M': {'name': 'Croatia', 'country': 'HR', 'timezone': 'Europe/Zagreb'},
            '10YCY-1001A0003J': {'name': 'Cyprus', 'country': 'CY', 'timezone': 'Asia/Nicosia'},
            '10YCZ-CEPS-----N': {'name': 'Czech Republic', 'country': 'CZ', 'timezone': 'Europe/Prague'},
            '10Y1001A1001A65H': {'name': 'Denmark East (DK1)', 'country': 'DK', 'timezone': 'Europe/Copenhagen'},
            '10Y1001A1001A39I': {'name': 'Denmark West (DK2)', 'country': 'DK', 'timezone': 'Europe/Copenhagen'},
            '10Y1001A1001A44P': {'name': 'Estonia', 'country': 'EE', 'timezone': 'Europe/Tallinn'},
            '10YFI-1--------U': {'name': 'Finland', 'country': 'FI', 'timezone': 'Europe/Helsinki'},
            '10YFR-RTE------C': {'name': 'France', 'country': 'FR', 'timezone': 'Europe/Paris'},
            '10Y1001A1001A83F': {'name': 'Germany', 'country': 'DE', 'timezone': 'Europe/Berlin'},
            '10YGR-HTSO-----Y': {'name': 'Greece', 'country': 'GR', 'timezone': 'Europe/Athens'},
            '10YHU-MAVIR----U': {'name': 'Hungary', 'country': 'HU', 'timezone': 'Europe/Budapest'},
            '10Y1001A1001A59C': {'name': 'Ireland', 'country': 'IE', 'timezone': 'Europe/Dublin'},
            '10YIT-GRTN-----B': {'name': 'Italy', 'country': 'IT', 'timezone': 'Europe/Rome'},
            '10YLV-1001A00074': {'name': 'Latvia', 'country': 'LV', 'timezone': 'Europe/Riga'},
            '10YLT-1001A0008Q': {'name': 'Lithuania', 'country': 'LT', 'timezone': 'Europe/Vilnius'},
            '10YLU-CEGEDEL-NQ': {'name': 'Luxembourg', 'country': 'LU', 'timezone': 'Europe/Luxembourg'},
            '10Y1001A1001A93C': {'name': 'Malta', 'country': 'MT', 'timezone': 'Europe/Malta'},
            '10YNL----------L': {'name': 'Netherlands', 'country': 'NL', 'timezone': 'Europe/Amsterdam'},
            '10YPL-AREA-----S': {'name': 'Poland', 'country': 'PL', 'timezone': 'Europe/Warsaw'},
            '10YPT-REN------W': {'name': 'Portugal', 'country': 'PT', 'timezone': 'Europe/Lisbon'},
            '10YRO-TEL------P': {'name': 'Romania', 'country': 'RO', 'timezone': 'Europe/Bucharest'},
            '10YSK-SEPS-----K': {'name': 'Slovakia', 'country': 'SK', 'timezone': 'Europe/Bratislava'},
            '10YSI-ELES-----O': {'name': 'Slovenia', 'country': 'SI', 'timezone': 'Europe/Ljubljana'},
            '10YES-REE------0': {'name': 'Spain', 'country': 'ES', 'timezone': 'Europe/Madrid'},
            '10YSE-1--------K': {'name': 'Sweden', 'country': 'SE', 'timezone': 'Europe/Stockholm'}
        }
        
        # Mapowanie typów generacji
        self.generation_types = {
            'B01': 'Biomass',
            'B02': 'Fossil Brown coal/Lignite',
            'B03': 'Fossil Coal-derived gas',
            'B04': 'Fossil Gas',
            'B05': 'Fossil Hard coal',
            'B06': 'Fossil Oil',
            'B09': 'Geothermal',
            'B10': 'Hydro Pumped Storage',
            'B11': 'Hydro Run-of-river and poundage',
            'B12': 'Hydro Water Reservoir',
            'B13': 'Marine',
            'B14': 'Nuclear',
            'B15': 'Other renewable',
            'B16': 'Solar',
            'B17': 'Waste',
            'B18': 'Wind Offshore',
            'B19': 'Wind Onshore',
            'B20': 'Other'
        }
    
    def _make_request(self, params: Dict, max_retries: int = 3) -> Optional[str]:
        """
        Wykonanie zapytania do API z retry logic
        
        Args:
            params: Parametry zapytania
            max_retries: Maksymalna liczba prób
            
        Returns:
            Odpowiedź XML jako string lub None w przypadku błędu
        """
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Making request with params: {params}")
                response = requests.get(self.base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.text
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
    
    def _parse_xml_response(self, xml_content: str) -> List[Dict]:
        """
        Parsowanie odpowiedzi XML do listy słowników
        
        Args:
            xml_content: Zawartość XML
            
        Returns:
            Lista słowników z danymi
        """
        if not xml_content:
            self.logger.error("Empty XML content provided to _parse_xml_response")
            return []
            
        try:
            self.logger.info(f"Parsing XML response of length {len(xml_content)}")
            root = ET.fromstring(xml_content)
            
            # Wykryj przestrzeń nazw
            namespace = ''
            if '}' in root.tag:
                namespace = root.tag.split('}')[0] + '}'
                self.logger.info(f"Detected namespace: {namespace}")
                
                # Utwórz mapowanie przestrzeni nazw dla findall
                namespaces = {'ns': namespace[1:-1]}  # Usuń znaki { i }
                self.logger.info(f"Using namespace mapping: {namespaces}")
            else:
                self.logger.info("No namespace detected in XML")
            
            data = []
            
            # Szukaj elementów TimeSeries z uwzględnieniem przestrzeni nazw
            if namespace:
                # Używamy xpath z przestrzenią nazw
                timeseries_elements = root.findall(".//ns:TimeSeries", namespaces)
                self.logger.info(f"Found {len(timeseries_elements)} TimeSeries elements using namespace")
            else:
                # Próba bez przestrzeni nazw
                timeseries_elements = root.findall(".//TimeSeries")
                self.logger.info(f"Found {len(timeseries_elements)} TimeSeries elements without namespace")
            
            # Jeśli nadal nie znaleziono, spróbuj inaczej
            if not timeseries_elements:
                # Ostatnia próba - przeszukaj wszystkie potomne elementy
                all_elements = list(root.iter())
                timeseries_elements = [elem for elem in all_elements if elem.tag.endswith('TimeSeries')]
                self.logger.info(f"Found {len(timeseries_elements)} TimeSeries elements using iter method")
            
            if not timeseries_elements:
                self.logger.warning("No TimeSeries elements found in XML response")
                # Zapisz fragment XML do analizy
                try:
                    with open("failed_xml_parse.xml", "w", encoding="utf-8") as f:
                        f.write(xml_content[:2000])  # Zapisz początek dla analizy
                    self.logger.info("First 2000 characters of XML saved to failed_xml_parse.xml")
                except Exception as e:
                    self.logger.error(f"Error saving XML fragment: {str(e)}")
                return []
            
            # Przetwarzanie każdego elementu TimeSeries
            for ts_idx, timeseries in enumerate(timeseries_elements):
                # Pobierz informacje o strefie przetargowej
                if namespace:
                    bidding_zone_elem = timeseries.find(".//ns:outBiddingZone_Domain.mRID", namespaces)
                    if bidding_zone_elem is None:
                        bidding_zone_elem = timeseries.find(".//ns:in_Domain.mRID", namespaces)
                else:
                    bidding_zone_elem = timeseries.find(".//outBiddingZone_Domain.mRID")
                    if bidding_zone_elem is None:
                        bidding_zone_elem = timeseries.find(".//in_Domain.mRID")
                
                bidding_zone = bidding_zone_elem.text if bidding_zone_elem is not None else None
                self.logger.info(f"TimeSeries {ts_idx} - Bidding zone: {bidding_zone}")
                
                # Pobierz informacje o typie generacji (jeśli dostępne)
                generation_type = None
                if namespace:
                    psr_type_elem = timeseries.find(".//ns:MktPSRType/ns:psrType", namespaces)
                else:
                    psr_type_elem = timeseries.find(".//MktPSRType/psrType")
                
                if psr_type_elem is not None:
                    generation_type = psr_type_elem.text
                    self.logger.info(f"TimeSeries {ts_idx} - Generation type: {generation_type}")
                
                # Pobierz okres czasu
                if namespace:
                    period_elements = timeseries.findall(".//ns:Period", namespaces)
                else:
                    period_elements = timeseries.findall(".//Period")
                
                self.logger.info(f"TimeSeries {ts_idx} has {len(period_elements)} Period elements")
                
                # Przetwarzanie każdego okresu
                for period_idx, period in enumerate(period_elements):
                    # Pobierz interwał czasu
                    if namespace:
                        start_time_elem = period.find(".//ns:start", namespaces)
                    else:
                        start_time_elem = period.find(".//start")
                    
                    if start_time_elem is None:
                        self.logger.warning(f"No start time found for Period {period_idx} in TimeSeries {ts_idx}")
                        continue
                    
                    start_time_text = start_time_elem.text
                    self.logger.info(f"Period {period_idx} - Start time: {start_time_text}")
                    
                    # Konwersja czasu UTC na datetime
                    start_datetime = datetime.fromisoformat(start_time_text.replace('Z', '+00:00'))
                    
                    # Pobierz punkty danych
                    if namespace:
                        point_elements = period.findall(".//ns:Point", namespaces)
                    else:
                        point_elements = period.findall(".//Point")
                    
                    self.logger.info(f"Period {period_idx} has {len(point_elements)} Point elements")
                    
                    # Przetwarzanie każdego punktu
                    for point in point_elements:
                        # Pobierz pozycję i ilość
                        if namespace:
                            position_elem = point.find(".//ns:position", namespaces)
                            quantity_elem = point.find(".//ns:quantity", namespaces)
                        else:
                            position_elem = point.find("position")
                            quantity_elem = point.find("quantity")
                        
                        if position_elem is not None and quantity_elem is not None:
                            position = int(position_elem.text)
                            quantity = float(quantity_elem.text)
                            
                            # Obliczenie czasu dla punktu
                            point_time = start_datetime + timedelta(hours=position - 1)
                            
                            # Stwórz rekord danych
                            record = {
                                'timestamp': point_time,
                                'quantity': quantity
                            }
                            
                            # Dodaj strefę przetargową, jeśli dostępna
                            if bidding_zone:
                                record['bidding_zone'] = bidding_zone
                            
                            # Dodaj typ generacji, jeśli dostępny
                            if generation_type:
                                record['generation_type'] = generation_type
                            
                            data.append(record)
            
            self.logger.info(f"Parsed {len(data)} data points from XML")
            return data
            
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing XML: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return []
    
    def get_actual_total_load(self, bidding_zone: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Pobieranie danych o rzeczywistym zużyciu energii
        
        Args:
            bidding_zone: Kod strefy przetargowej (EIC)
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            DataFrame z danymi o zużyciu
        """
        self.logger.info(f"Fetching actual load data for {bidding_zone}")
        
        # Przygotowanie parametrów zapytania
        params = {
            'securityToken': self.security_token,
            'documentType': 'A65',  # Actual total load
            'processType': 'A16',   # Realised
            'outBiddingZone_Domain': bidding_zone,
            'periodStart': start_date.strftime('%Y%m%d%H00'),
            'periodEnd': end_date.strftime('%Y%m%d%H00')
        }
        
        # Wykonanie zapytania
        self.logger.info(f"Making request with params: {params}")
        xml_content = self._make_request(params)
        
        if not xml_content:
            self.logger.error(f"Failed to fetch data for {bidding_zone}")
            return pd.DataFrame()
        
        self.logger.info(f"Received response of length {len(xml_content)} characters")
        
        # Parsowanie odpowiedzi
        data = self._parse_xml_response(xml_content)
        if not data:
            self.logger.warning(f"No data parsed from XML response for {bidding_zone}")
            return pd.DataFrame()
        
        self.logger.info(f"Parsed {len(data)} records from XML")
        
        # Konwersja do DataFrame
        df = pd.DataFrame(data)
        df['data_type'] = 'actual_load'
        
        self.logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        if not df.empty:
            self.logger.info(f"DataFrame columns: {df.columns.tolist()}")
        
        return df
    
    def get_actual_generation_per_type(self, bidding_zone: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Pobieranie danych o rzeczywistej generacji wg typów
        
        Args:
            bidding_zone: Kod strefy przetargowej (EIC)
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            DataFrame z danymi o generacji wg typów
        """
        self.logger.info(f"Fetching generation data for {bidding_zone}")
        
        # Przygotowanie parametrów zapytania
        params = {
            'securityToken': self.security_token,
            'documentType': 'A75',  # Actual generation per type
            'processType': 'A16',   # Realised
            'in_Domain': bidding_zone,
            'periodStart': start_date.strftime('%Y%m%d%H00'),
            'periodEnd': end_date.strftime('%Y%m%d%H00')
        }
        
        # Wykonanie zapytania
        self.logger.info(f"Making request with params: {params}")
        xml_content = self._make_request(params)
        
        if not xml_content:
            self.logger.error(f"Failed to fetch generation data for {bidding_zone}")
            return pd.DataFrame()
        
        self.logger.info(f"Received response of length {len(xml_content)} characters")
        
        # Parsowanie odpowiedzi
        data = self._parse_xml_response(xml_content)
        if not data:
            self.logger.warning(f"No generation data parsed from XML response for {bidding_zone}")
            return pd.DataFrame()
        
        self.logger.info(f"Parsed {len(data)} generation records from XML")
        
        # Konwersja do DataFrame
        df = pd.DataFrame(data)
        df['data_type'] = 'generation'
        
        self.logger.info(f"Created generation DataFrame with {len(df)} rows and {len(df.columns)} columns")
        if not df.empty:
            self.logger.info(f"Generation DataFrame columns: {df.columns.tolist()}")
            
            # Sprawdź, czy mamy dane o typach generacji
            if 'generation_type' in df.columns:
                type_counts = df['generation_type'].value_counts()
                self.logger.info(f"Generation types distribution: {type_counts.to_dict()}")
            else:
                self.logger.warning("No generation_type column in the DataFrame")
        
        return df
    
    def get_day_ahead_forecast(self, bidding_zone: str, start_date: datetime, 
                              end_date: datetime) -> pd.DataFrame:
        """
        Pobieranie prognoz zużycia na dzień naprzód
        
        Args:
            bidding_zone: Kod strefy przetargowej (EIC)
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            DataFrame z prognozami zużycia
        """
        self.logger.info(f"Fetching day-ahead forecast for {bidding_zone}")
        
        params = {
            'securityToken': self.security_token,
            'documentType': 'A65',  # System total load
            'processType': 'A01',   # Day ahead
            'outBiddingZone_Domain': bidding_zone,
            'periodStart': start_date.strftime('%Y%m%d%H00'),
            'periodEnd': end_date.strftime('%Y%m%d%H00')
        }
        
        # Wykonanie zapytania
        self.logger.info(f"Making request with params: {params}")
        xml_content = self._make_request(params)
        
        if not xml_content:
            self.logger.error(f"Failed to fetch forecast data for {bidding_zone}")
            return pd.DataFrame()
        
        self.logger.info(f"Received forecast response of length {len(xml_content)} characters")
        
        # Parsowanie odpowiedzi
        data = self._parse_xml_response(xml_content)
        if not data:
            self.logger.warning(f"No forecast data parsed from XML response for {bidding_zone}")
            return pd.DataFrame()
        
        self.logger.info(f"Parsed {len(data)} forecast records from XML")
        
        # Konwersja do DataFrame
        df = pd.DataFrame(data)
        df['data_type'] = 'forecast'
        
        self.logger.info(f"Created forecast DataFrame with {len(df)} rows and {len(df.columns)} columns")
        if not df.empty:
            self.logger.info(f"Forecast DataFrame columns: {df.columns.tolist()}")
            
            # Sprawdź zakres dat w danych
            if 'timestamp' in df.columns:
                min_date = df['timestamp'].min()
                max_date = df['timestamp'].max()
                self.logger.info(f"Forecast data date range: {min_date} to {max_date}")
        
        return df
    
    def extract_all_data(self, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        Ekstrakcja wszystkich danych dla wszystkich stref
        
        Args:
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            Słownik z DataFrame'ami dla różnych typów danych
        """
        self.logger.info(f"Starting ENTSO-E data extraction from {start_date} to {end_date}")
        
        # Logowanie rozpoczęcia procesu
        self._log_process('ENTSO-E_EXTRACTION', 'RUNNING')
        
        all_data = {
            'actual_load': [],
            'generation': [],
            'forecast': []
        }
        
        total_zones = len(self.bidding_zones)
        processed_zones = 0
        
        try:
            for zone_code, zone_info in self.bidding_zones.items():
                self.logger.info(f"Processing zone: {zone_info['name']} ({zone_code})")
                
                # Pobieranie danych o zużyciu
                load_data = self.get_actual_total_load(zone_code, start_date, end_date)
                if not load_data.empty:
                    self.logger.info(f"Got {len(load_data)} rows of actual load data for {zone_code}")
                    load_data['zone_code'] = zone_code
                    load_data['zone_name'] = zone_info['name']
                    load_data['country'] = zone_info['country']
                    all_data['actual_load'].append(load_data)
                else:
                    self.logger.warning(f"No actual load data received for {zone_code}")
                
                # Pobieranie danych o generacji
                gen_data = self.get_actual_generation_per_type(zone_code, start_date, end_date)
                if not gen_data.empty:
                    self.logger.info(f"Got {len(gen_data)} rows of generation data for {zone_code}")
                    gen_data['zone_code'] = zone_code
                    gen_data['zone_name'] = zone_info['name']
                    gen_data['country'] = zone_info['country']
                    all_data['generation'].append(gen_data)
                else:
                    self.logger.warning(f"No generation data received for {zone_code}")
                
                # Pobieranie prognoz
                forecast_data = self.get_day_ahead_forecast(zone_code, start_date, end_date)
                if not forecast_data.empty:
                    self.logger.info(f"Got {len(forecast_data)} rows of forecast data for {zone_code}")
                    forecast_data['zone_code'] = zone_code
                    forecast_data['zone_name'] = zone_info['name']
                    forecast_data['country'] = zone_info['country']
                    all_data['forecast'].append(forecast_data)
                else:
                    self.logger.warning(f"No forecast data received for {zone_code}")
                
                processed_zones += 1
                self.logger.info(f"Processed {processed_zones}/{total_zones} zones")
                
                # Przerwa między zapytaniami aby nie przekroczyć limitów API
                time.sleep(1)
            
            # Łączenie danych
            final_data = {}
            total_records = 0
            
            for data_type, data_list in all_data.items():
                self.logger.info(f"Processing {data_type}: {len(data_list)} DataFrames to combine")
                
                if data_list:
                    # Sprawdź, czy każdy DataFrame ma jakieś dane
                    for i, df in enumerate(data_list):
                        self.logger.info(f"DataFrame {i} for {data_type} has {len(df)} rows")
                    
                    combined_df = pd.concat(data_list, ignore_index=True)
                    final_data[data_type] = combined_df
                    total_records += len(combined_df)
                    self.logger.info(f"Combined {len(combined_df)} records for {data_type}")
                    
                    # Pokaż przykładowy rekord, jeśli dostępny
                    if not combined_df.empty:
                        self.logger.info(f"Sample record from {data_type}:")
                        self.logger.info(combined_df.iloc[0].to_dict())
                else:
                    self.logger.warning(f"No DataFrames to combine for {data_type}")
                    final_data[data_type] = pd.DataFrame()
            
            # Logowanie sukcesu
            self._log_process('ENTSO-E_EXTRACTION', 'SUCCESS', total_records)
            
            return final_data
            
        except Exception as e:
            self.logger.error(f"Error during data extraction: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            self._log_process('ENTSO-E_EXTRACTION', 'FAILED', 0, str(e))
            raise
    
    def save_to_staging(self, data: Dict[str, pd.DataFrame]) -> bool:
        """
        Zapisanie danych do tabel staging w bazie danych
        
        Args:
            data: Słownik z DataFrame'ami
            
        Returns:
            True jeśli sukces, False w przeciwnym razie
        """
        try:
            self.logger.info(f"Attempting to connect to database with connection string: {self.connection_string[:20]}...")
            conn = pyodbc.connect(self.connection_string)
            self.logger.info("Database connection successful")
            
            for data_type, df in data.items():
                if df.empty:
                    self.logger.info(f"No data for {data_type}, skipping")
                    continue
                
                table_name = f"staging_entso_{data_type}"
                
                # Próba utworzenia tabeli
                try:
                    self.logger.info(f"Creating table {table_name} if not exists")
                    # Utworzenie tabeli staging jeśli nie istnieje
                    self._create_staging_table(conn, table_name, df)
                    self.logger.info(f"Table creation for {table_name} completed")
                except Exception as table_e:
                    self.logger.error(f"Error creating table {table_name}: {str(table_e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    
                    # Zapisz dane do CSV jako plan awaryjny
                    try:
                        csv_filename = f"{table_name}.csv"
                        df.to_csv(csv_filename, index=False)
                        self.logger.info(f"Data saved to {csv_filename} as fallback due to table creation error")
                    except Exception as csv_e:
                        self.logger.error(f"Error saving to CSV: {str(csv_e)}")
                    
                    continue  # Przejdź do następnego typu danych
                
                # Próba czyszczenia tabeli
                try:
                    self.logger.info(f"Truncating table {table_name}")
                    cursor = conn.cursor()
                    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL TRUNCATE TABLE {table_name}")
                    conn.commit()
                    self.logger.info(f"Table {table_name} truncated successfully")
                except Exception as trunc_e:
                    self.logger.error(f"Error truncating table {table_name}: {str(trunc_e)}")
                    conn.rollback()
                    # Kontynuuj bez czyszczenia - może się uda wstawić dane
                
                # Wstawianie danych
                try:
                    self.logger.info(f"Inserting {len(df)} records into {table_name}")
                    self._bulk_insert_dataframe(conn, table_name, df)
                    self.logger.info(f"Data insertion to {table_name} completed")
                except Exception as insert_e:
                    self.logger.error(f"Error inserting data into {table_name}: {str(insert_e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    
                    # Zapisz dane do CSV jako plan awaryjny
                    try:
                        csv_filename = f"{table_name}.csv"
                        df.to_csv(csv_filename, index=False)
                        self.logger.info(f"Data saved to {csv_filename} as fallback due to insertion error")
                    except Exception as csv_e:
                        self.logger.error(f"Error saving to CSV: {str(csv_e)}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to staging: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            # Zapisz wszystkie dane do CSV jako plan awaryjny
            for data_type, df in data.items():
                if df.empty:
                    continue
                
                try:
                    csv_filename = f"staging_entso_{data_type}.csv"
                    df.to_csv(csv_filename, index=False)
                    self.logger.info(f"Data saved to {csv_filename} as fallback")
                except Exception as csv_e:
                    self.logger.error(f"Error saving to CSV: {str(csv_e)}")
            
            return False
    def _create_staging_table(self, conn, table_name: str, df: pd.DataFrame):
        """Utworzenie tabeli staging"""
        try:
            cursor = conn.cursor()
            
            # Podstawowe kolumny dla wszystkich tabel staging
            columns = [
                "id BIGINT IDENTITY(1,1) PRIMARY KEY",
                "timestamp DATETIME2 NOT NULL",
                "quantity DECIMAL(15,2) NOT NULL",
                "zone_code NVARCHAR(20) NOT NULL",
                "zone_name NVARCHAR(100) NOT NULL",
                "country NVARCHAR(5) NOT NULL",
                "data_type NVARCHAR(20) NOT NULL"
            ]
            
            # Dodatkowe kolumny dla danych generacji
            if 'generation' in table_name:
                columns.append("generation_type NVARCHAR(5) NULL")
            
            columns.append("created_at DATETIME2 DEFAULT GETDATE()")
            
            create_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
            BEGIN
                CREATE TABLE {table_name} (
                    {', '.join(columns)}
                );
                PRINT 'Table {table_name} created successfully';
            END
            """
            
            self.logger.info(f"Creating table {table_name} if not exists")
            self.logger.debug(f"SQL: {create_sql}")
            
            cursor.execute(create_sql)
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
            # Nie rzucaj wyjątku, aby skrypt mógł kontynuować działanie
            
    def _bulk_insert_dataframe(self, conn, table_name: str, df: pd.DataFrame):
        """Bulk insert DataFrame do tabeli"""
        try:
            cursor = conn.cursor()
            
            # Przygotowanie danych do wstawienia
            columns = ['timestamp', 'quantity', 'zone_code', 'zone_name', 'country', 'data_type']
            if 'generation_type' in df.columns:
                columns.append('generation_type')
            
            # Licznik wstawionych rekordów
            inserted_count = 0
            
            # Bulk insert
            for _, row in df.iterrows():
                try:
                    values = [row[col] if col in row else None for col in columns]
                    placeholders = ', '.join(['?' for _ in values])
                    
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(insert_sql, values)
                    inserted_count += 1
                    
                    # Commit co 1000 rekordów
                    if inserted_count % 1000 == 0:
                        conn.commit()
                        self.logger.info(f"Inserted {inserted_count} records into {table_name}")
                        
                except Exception as e:
                    self.logger.error(f"Error inserting row: {str(e)}")
                    # Kontynuuj z następnym wierszem
            
            # Commit pozostałych rekordów
            conn.commit()
            self.logger.info(f"Total {inserted_count} records inserted into {table_name}")
            
            # Jako plan awaryjny, zapisz również dane do CSV
            try:
                csv_filename = f"{table_name}.csv"
                df.to_csv(csv_filename, index=False)
                self.logger.info(f"Data also saved to {csv_filename} as backup")
            except Exception as csv_e:
                self.logger.error(f"Error saving to CSV: {str(csv_e)}")
                
        except Exception as e:
            self.logger.error(f"Error in bulk insert: {str(e)}")
            
            # Plan awaryjny: zapisz dane do CSV
            try:
                csv_filename = f"{table_name}.csv"
                df.to_csv(csv_filename, index=False)
                self.logger.info(f"Data saved to {csv_filename} as fallback")
            except Exception as csv_e:
                self.logger.error(f"Error saving to CSV: {str(csv_e)}")
    
    def _log_process(self, process_name: str, status: str, records: int = 0, error_msg: str = None):
        """Logowanie procesu do bazy danych"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            cursor.execute("""
                IF OBJECT_ID('dbo.sp_log_etl_process', 'P') IS NOT NULL
                    EXEC sp_log_etl_process ?, ?, ?, ?
                ELSE
                BEGIN
                    PRINT 'Stored procedure sp_log_etl_process not found, logging to console instead';
                END
            """, (process_name, status, records, error_msg))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging process: {str(e)}")

# Funkcja główna do uruchomienia z SSIS
def main():
    """Główna funkcja wywoływana przez SSIS"""
    # Konfiguracja logowania
    import sys
    import os
    import argparse
    from datetime import datetime, timedelta
    
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("ENTSOEMain")
    
    logger.info("Starting ENTSOEClient process")
    
    # Ręczne parsowanie argumentów (bez argparse)
    token = None
    connection_string = None
    start_date_str = None
    end_date_str = None
    
    # Parsowanie argumentów ręcznie
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i].startswith('--token'):
            if '=' in args[i]:
                token = args[i].split('=', 1)[1]
            elif i + 1 < len(args):
                token = args[i + 1]
                i += 1
        elif args[i].startswith('--connection'):
            if '=' in args[i]:
                connection_string = args[i].split('=', 1)[1]
            elif i + 1 < len(args):
                connection_string = args[i + 1]
                i += 1
        elif args[i].startswith('--start'):
            if '=' in args[i]:
                start_date_str = args[i].split('=', 1)[1]
            elif i + 1 < len(args):
                start_date_str = args[i + 1]
                i += 1
        elif args[i].startswith('--end'):
            if '=' in args[i]:
                end_date_str = args[i].split('=', 1)[1]
            elif i + 1 < len(args):
                end_date_str = args[i + 1]
                i += 1
        i += 1
    
    # Jeśli nie podano argumentów, spróbuj użyć zmiennych środowiskowych
    if not token:
        token = os.getenv('ENTSO_TOKEN', '')
    
    if not connection_string:
        connection_string = os.getenv('DW_CONNECTION_STRING', '')
    
    # Sprawdzenie czy wymagane argumenty są dostępne
    if not token:
        logger.error("ENTSO-E API token is missing. Provide --token argument or set ENTSO_TOKEN environment variable")
        sys.exit(1)
    
    if not connection_string:
        logger.error("Database connection string is missing. Provide --connection argument or set DW_CONNECTION_STRING environment variable")
        sys.exit(1)
        
    logger.info(f"Security token found: {'Yes' if token else 'No'}")
    logger.info(f"Connection string found: {'Yes' if connection_string else 'No'}")
    
    # Domyślnie pobieramy dane z ostatnich 24 godzin
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(hours=24)
    
    # Przetwarzanie dat z argumentów
    if start_date_str:
        try:
            logger.info(f"Parsing start date: {start_date_str}")
            # Obsługa różnych formatów daty
            if 'T' in start_date_str:  # Format ISO z czasem
                start_date = datetime.fromisoformat(start_date_str)
            else:  # Tylko data
                start_date = datetime.fromisoformat(start_date_str + 'T00:00:00')
            logger.info(f"Using custom start date: {start_date}")
        except ValueError as e:
            logger.error(f"Invalid start date format: {e}")
            sys.exit(1)
            
    if end_date_str:
        try:
            logger.info(f"Parsing end date: {end_date_str}")
            # Obsługa różnych formatów daty
            if 'T' in end_date_str:  # Format ISO z czasem
                end_date = datetime.fromisoformat(end_date_str)
            else:  # Tylko data
                end_date = datetime.fromisoformat(end_date_str + 'T00:00:00')
            logger.info(f"Using custom end date: {end_date}")
        except ValueError as e:
            logger.error(f"Invalid end date format: {e}")
            sys.exit(1)
    
    logger.info(f"Data extraction period: {start_date} to {end_date}")
    
    try:
        # Inicjalizacja klienta
        client = ENTSOEClient(token, connection_string)
        
        # Ekstrakcja danych
        data = client.extract_all_data(start_date, end_date)
        
        for data_type, df in data.items():
            if df.empty:
                logger.warning(f"DataFrame for {data_type} is empty!")
            else:
                logger.info(f"DataFrame for {data_type} contains {len(df)} rows and {len(df.columns)} columns")
                logger.info(f"Columns: {df.columns.tolist()}")
                logger.info(f"First row sample: {df.iloc[0].to_dict()}")
            
        # Zapis do staging
        success = client.save_to_staging(data)
        
        if success:
            logger.info("ENTSO-E data extraction completed successfully")
            sys.exit(0)
        else:
            logger.error("Error occurred during data extraction")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()