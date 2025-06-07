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
    
    # Użyj argparse dla bardziej przyjaznego parsowania argumentów
    parser = argparse.ArgumentParser(description='Pobieranie danych z ENTSO-E Transparency Platform')
    parser.add_argument('--token', help='Token API dla ENTSO-E', default=os.getenv('ENTSO_TOKEN', ''))
    parser.add_argument('--connection', help='Connection string do bazy danych', default=os.getenv('DW_CONNECTION_STRING', ''))
    parser.add_argument('--start', help='Data początkowa (format: YYYY-MM-DD lub YYYY-MM-DDThh:mm:ss)')
    parser.add_argument('--end', help='Data końcowa (format: YYYY-MM-DD lub YYYY-MM-DDThh:mm:ss)')
    parser.add_argument('--eu-only', action='store_true', help='Pobieraj dane tylko dla krajów UE')
    parser.add_argument('--concurrent', type=int, default=3, help='Maksymalna liczba równoczesnych zapytań')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', 
                       help='Poziom logowania')
    
    # Obsługa przypadku gdy skrypt jest uruchamiany z SSIS (bez argumentów)
    if len(sys.argv) > 1:
        args = parser.parse_args()
    else:
        # Używamy wartości domyślnych z env lub hardcoded
        args = parser.parse_args([])
    
    # Konfiguracja poziomu logowania
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if isinstance(numeric_level, int):
        logging.getLogger().setLevel(numeric_level)
    
    # Sprawdzenie czy wymagane argumenty są dostępne
    if not args.token:
        logger.error("ENTSO-E API token is missing. Provide --token argument or set ENTSO_TOKEN environment variable")
        sys.exit(1)
    
    if not args.connection:
        logger.error("Database connection string is missing. Provide --connection argument or set DW_CONNECTION_STRING environment variable")
        sys.exit(1)
        
    logger.info(f"Security token found: {'Yes' if args.token else 'No'}")
    logger.info(f"Connection string found: {'Yes' if args.connection else 'No'}")
    logger.info(f"EU only mode: {args.eu_only}")
    logger.info(f"Concurrent requests: {args.concurrent}")
    
    # Domyślnie pobieramy dane z ostatnich 24 godzin
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(hours=24)
    
    # Przetwarzanie dat z argumentów
    if args.start:
        try:
            logger.info(f"Parsing start date: {args.start}")
            # Obsługa różnych formatów daty
            if 'T' in args.start:  # Format ISO z czasem
                start_date = datetime.fromisoformat(args.start)
            else:  # Tylko data
                start_date = datetime.fromisoformat(args.start + 'T00:00:00')
            logger.info(f"Using custom start date: {start_date}")
        except ValueError as e:
            logger.error(f"Invalid start date format: {e}")
            sys.exit(1)
            
    if args.end:
        try:
            logger.info(f"Parsing end date: {args.end}")
            # Obsługa różnych formatów daty
            if 'T' in args.end:  # Format ISO z czasem
                end_date = datetime.fromisoformat(args.end)
            else:  # Tylko data
                end_date = datetime.fromisoformat(args.end + 'T00:00:00')
            logger.info(f"Using custom end date: {end_date}")
        except ValueError as e:
            logger.error(f"Invalid end date format: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()


"""
ENTSOEClient.py
Klient API do komunikacji z ENTSO-E Transparency Platform
Pobiera dane o zużyciu energii i generacji wg typów źródeł
Ulepszony o obsługę wszystkich krajów UE
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
from concurrent.futures import ThreadPoolExecutor, as_completed

class ENTSOEClient:
    """Klient API dla ENTSO-E Transparency Platform"""
    
    def __init__(self, security_token: str, connection_string: str, max_concurrent_requests: int = 3):
        """
        Inicjalizacja klienta ENTSO-E
        
        Args:
            security_token: Token API dla ENTSO-E
            connection_string: String połączenia z bazą danych
            max_concurrent_requests: Maksymalna liczba równoczesnych zapytań
        """
        self.security_token = security_token
        self.base_url = "https://web-api.tp.entsoe.eu/api"
        self.connection_string = connection_string
        self.max_concurrent_requests = max_concurrent_requests
        self.rate_limit_delay = 1.0  # Domyślne opóźnienie pomiędzy zapytaniami (w sekundach)
        
        # Konfiguracja logowania
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Logowanie parametrów (bez wrażliwych danych)
        self.logger.info(f"Initializing ENTSOEClient with connection to {self.base_url}")
        
        # Kompletna lista stref przetargowych dla krajów UE i obszaru ENTSO-E
        self.bidding_zones = {
            # Kraje Unii Europejskiej
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
            '10YSE-1--------K': {'name': 'Sweden', 'country': 'SE', 'timezone': 'Europe/Stockholm'},
            
            # Dodatkowe strefy dla krajów z wieloma strefami przetargowymi
            '10Y1001A1001A45N': {'name': 'Sweden SE1', 'country': 'SE', 'timezone': 'Europe/Stockholm'},
            '10Y1001A1001A46L': {'name': 'Sweden SE2', 'country': 'SE', 'timezone': 'Europe/Stockholm'},
            '10Y1001A1001A47J': {'name': 'Sweden SE3', 'country': 'SE', 'timezone': 'Europe/Stockholm'},
            '10Y1001A1001A48H': {'name': 'Sweden SE4', 'country': 'SE', 'timezone': 'Europe/Stockholm'},
            '10Y1001A1001A73I': {'name': 'Italy North', 'country': 'IT', 'timezone': 'Europe/Rome'},
            '10Y1001A1001A70O': {'name': 'Italy Centre North', 'country': 'IT', 'timezone': 'Europe/Rome'},
            '10Y1001A1001A71M': {'name': 'Italy Centre South', 'country': 'IT', 'timezone': 'Europe/Rome'},
            '10Y1001A1001A72K': {'name': 'Italy South', 'country': 'IT', 'timezone': 'Europe/Rome'},
            '10Y1001A1001A788': {'name': 'Italy Sardinia', 'country': 'IT', 'timezone': 'Europe/Rome'},
            '10Y1001A1001A76C': {'name': 'Italy Sicily', 'country': 'IT', 'timezone': 'Europe/Rome'},
            
            # Dodatkowe kraje obszaru ENTSO-E spoza UE
            '10YCH-SWISSGRIDZ': {'name': 'Switzerland', 'country': 'CH', 'timezone': 'Europe/Zurich'},
            '10YNO-0--------C': {'name': 'Norway', 'country': 'NO', 'timezone': 'Europe/Oslo'},
            '10YGB----------A': {'name': 'United Kingdom', 'country': 'GB', 'timezone': 'Europe/London'},
            '10YCS-SERBIATSOV': {'name': 'Serbia', 'country': 'RS', 'timezone': 'Europe/Belgrade'},
            '10YMK-MEPSO----8': {'name': 'North Macedonia', 'country': 'MK', 'timezone': 'Europe/Skopje'},
            '10YAL-KESH-----5': {'name': 'Albania', 'country': 'AL', 'timezone': 'Europe/Tirane'},
            '10YBA-JPCC-----D': {'name': 'Bosnia and Herzegovina', 'country': 'BA', 'timezone': 'Europe/Sarajevo'},
            '10YME-CGES-----L': {'name': 'Montenegro', 'country': 'ME', 'timezone': 'Europe/Podgorica'},
            '10YTR-TEIAS----W': {'name': 'Turkey', 'country': 'TR', 'timezone': 'Europe/Istanbul'}
        }
        
        # Status zapytań API dla dynamicznego dostosowania rate limiting
        self.request_stats = {
            'success': 0,
            'rate_limit_hits': 0,
            'errors': 0,
            'last_adjustment': time.time()
        }
        
        # Mapowanie typów generacji
        self.generation_types = {
            'B01': 'Biomass',
            'B02': 'Fossil Brown coal/Lignite',
            'B03': 'Fossil Coal-derived gas',
            'B04': 'Fossil Gas',
            'B05': 'Fossil Hard coal',
            'B06': 'Fossil Oil',
            'B07': 'Fossil Oil shale',
            'B08': 'Fossil Peat',
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
                    self.request_stats['success'] += 1
                    self._adjust_rate_limiting()
                    return response.text
                elif response.status_code == 429:  # Too Many Requests
                    self.request_stats['rate_limit_hits'] += 1
                    wait_time = 2 ** attempt * self.rate_limit_delay
                    self.logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    self._adjust_rate_limiting()
                else:
                    self.request_stats['errors'] += 1
                    error_msg = f"HTTP Error {response.status_code}"
                    try:
                        # Próba odczytania komunikatu błędu z XML
                        root = ET.fromstring(response.text)
                        reason = root.find(".//Reason")
                        if reason is not None:
                            error_msg += f": {reason.text}"
                    except:
                        error_msg += f": {response.text[:100]}"
                    
                    self.logger.error(error_msg)
                    
            except requests.exceptions.RequestException as e:
                self.request_stats['errors'] += 1
                self.logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        return None
    
    def _adjust_rate_limiting(self):
        """Dynamicznie dostosowuje opóźnienie między zapytaniami w oparciu o statystyki"""
        now = time.time()
        # Sprawdź tylko co 10 zapytań lub co minutę
        total_requests = self.request_stats['success'] + self.request_stats['rate_limit_hits'] + self.request_stats['errors']
        if (total_requests % 10 == 0) or (now - self.request_stats['last_adjustment'] > 60):
            # Jeśli ponad 10% zapytań trafia w limit, zwiększ opóźnienie
            if self.request_stats['rate_limit_hits'] > 0 and (self.request_stats['rate_limit_hits'] / total_requests) > 0.1:
                self.rate_limit_delay = min(5.0, self.rate_limit_delay * 1.5)
                self.logger.info(f"Rate limit delay increased to {self.rate_limit_delay}s due to rate limiting")
            # Jeśli mniej niż 1% zapytań trafia w limit, zmniejsz opóźnienie
            elif (total_requests > 20) and (self.request_stats['rate_limit_hits'] / total_requests) < 0.01:
                self.rate_limit_delay = max(0.5, self.rate_limit_delay * 0.9)
                self.logger.info(f"Rate limit delay decreased to {self.rate_limit_delay}s due to good performance")
            
            self.request_stats['last_adjustment'] = now
    
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
    
    def _process_zone(self, zone_code: str, zone_info: Dict, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        Procesowanie danych dla pojedynczej strefy
        
        Args:
            zone_code: Kod strefy przetargowej
            zone_info: Informacje o strefie
            start_date: Data rozpoczęcia
            end_date: Data zakończenia
            
        Returns:
            Słownik z DataFramami dla różnych typów danych
        """
        self.logger.info(f"Processing zone: {zone_info['name']} ({zone_code})")
        
        zone_data = {
            'actual_load': pd.DataFrame(),
            'generation': pd.DataFrame(),
            'forecast': pd.DataFrame()
        }
        
        try:
            # Pobieranie danych o zużyciu
            load_data = self.get_actual_total_load(zone_code, start_date, end_date)
            if not load_data.empty:
                self.logger.info(f"Got {len(load_data)} rows of actual load data for {zone_code}")
                load_data['zone_code'] = zone_code
                load_data['zone_name'] = zone_info['name']
                load_data['country'] = zone_info['country']
                zone_data['actual_load'] = load_data
            else:
                self.logger.warning(f"No actual load data received for {zone_code}")
            
            # Pobieranie danych o generacji
            gen_data = self.get_actual_generation_per_type(zone_code, start_date, end_date)
            if not gen_data.empty:
                self.logger.info(f"Got {len(gen_data)} rows of generation data for {zone_code}")
                gen_data['zone_code'] = zone_code
                gen_data['zone_name'] = zone_info['name']
                gen_data['country'] = zone_info['country']
                zone_data['generation'] = gen_data
            else:
                self.logger.warning(f"No generation data received for {zone_code}")
            
            # Pobieranie prognoz
            forecast_data = self.get_day_ahead_forecast(zone_code, start_date, end_date)
            if not forecast_data.empty:
                self.logger.info(f"Got {len(forecast_data)} rows of forecast data for {zone_code}")
                forecast_data['zone_code'] = zone_code
                forecast_data['zone_name'] = zone_info['name']
                forecast_data['country'] = zone_info['country']
                zone_data['forecast'] = forecast_data
            else:
                self.logger.warning(f"No forecast data received for {zone_code}")
                
        except Exception as e:
            self.logger.error(f"Error processing zone {zone_code}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
        
        return zone_data