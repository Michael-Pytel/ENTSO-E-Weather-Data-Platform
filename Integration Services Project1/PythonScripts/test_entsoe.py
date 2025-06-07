"""
ENTSO-E API Test Script
Prosty skrypt do testowania połączenia z API ENTSO-E
Testuje 3 główne metody API: actual_load, generation i forecast
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import logging
import sys

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ENTSO-E_API_Test")

# Podstawowe parametry
BASE_URL = "https://web-api.tp.entsoe.eu/api"
TEST_BIDDING_ZONE = '10YPL-AREA-----S'  # Poland

def make_request(url, params, description):
    """
    Wykonuje zapytanie do API i wyświetla szczegóły odpowiedzi
    """
    logger.info(f"Testing {description}")
    logger.info(f"Request parameters: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        logger.info(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            response_text = response.text
            logger.info(f"Response length: {len(response_text)} characters")
            logger.info(f"Response preview: {response_text[:200]}...")
            
            # Próba parsowania XML
            try:
                root = ET.fromstring(response_text)
                logger.info(f"XML root tag: {root.tag}")
                
                # Sprawdź czy XML zawiera TimeSeries
                namespaces = {
                    '': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0'
                }
                timeseries_elements = root.findall('.//TimeSeries', namespaces)
                logger.info(f"Found {len(timeseries_elements)} TimeSeries elements")
                
                if timeseries_elements:
                    # Sprawdź pierwszy TimeSeries
                    first_ts = timeseries_elements[0]
                    
                    # Sprawdź czy zawiera Period
                    period_elements = first_ts.findall('.//Period', namespaces)
                    logger.info(f"First TimeSeries has {len(period_elements)} Period elements")
                    
                    if period_elements:
                        # Sprawdź punkty w pierwszym okresie
                        first_period = period_elements[0]
                        point_elements = first_period.findall('.//Point', namespaces)
                        logger.info(f"First Period has {len(point_elements)} Point elements")
                        
                        if point_elements:
                            # Pokaż przykładowy punkt
                            first_point = point_elements[0]
                            position = first_point.find('position', namespaces)
                            quantity = first_point.find('quantity', namespaces)
                            
                            if position is not None and quantity is not None:
                                logger.info(f"Sample point - position: {position.text}, quantity: {quantity.text}")
                            else:
                                logger.warning("Point does not contain position or quantity")
                        else:
                            logger.warning("No Point elements found in Period")
                    else:
                        logger.warning("No Period elements found in TimeSeries")
                else:
                    logger.warning("No TimeSeries elements found in response")
                    # Sprawdź, czy w odpowiedzi jest komunikat o błędzie
                    reason = root.find('.//Reason/text', namespaces)
                    if reason is not None:
                        logger.error(f"API returned error: {reason.text}")
                
                return True
            except ET.ParseError as e:
                logger.error(f"XML parsing error: {str(e)}")
                logger.error(f"Raw response: {response_text}")
                return False
        else:
            logger.error(f"HTTP Error {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return False

def test_actual_load(token, bidding_zone, start_date, end_date):
    """
    Test pobierania danych o rzeczywistym zużyciu energii
    """
    params = {
        'securityToken': token,
        'documentType': 'A65',  # Actual total load
        'processType': 'A16',   # Realised
        'outBiddingZone_Domain': bidding_zone,
        'periodStart': start_date.strftime('%Y%m%d%H00'),
        'periodEnd': end_date.strftime('%Y%m%d%H00')
    }
    
    return make_request(BASE_URL, params, "Actual Load API")

def test_generation_per_type(token, bidding_zone, start_date, end_date):
    """
    Test pobierania danych o rzeczywistej generacji wg typów
    """
    params = {
        'securityToken': token,
        'documentType': 'A75',  # Actual generation per type
        'processType': 'A16',   # Realised
        'in_Domain': bidding_zone,
        'periodStart': start_date.strftime('%Y%m%d%H00'),
        'periodEnd': end_date.strftime('%Y%m%d%H00')
    }
    
    return make_request(BASE_URL, params, "Generation Per Type API")

def test_day_ahead_forecast(token, bidding_zone, start_date, end_date):
    """
    Test pobierania prognoz zużycia na dzień naprzód
    """
    params = {
        'securityToken': token,
        'documentType': 'A65',  # System total load
        'processType': 'A01',   # Day ahead
        'outBiddingZone_Domain': bidding_zone,
        'periodStart': start_date.strftime('%Y%m%d%H00'),
        'periodEnd': end_date.strftime('%Y%m%d%H00')
    }
    
    return make_request(BASE_URL, params, "Day Ahead Forecast API")

def test_with_historical_data(token):
    """
    Test z danymi historycznymi (przeszłość, nie przyszłość)
    """
    logger.info("=== Testing with HISTORICAL data (past dates) ===")
    
    # Użyj dat z przeszłości (np. rok 2023)
    end_date = datetime(2023, 5, 1)
    start_date = datetime(2023, 4, 1)  # Miesiąc danych
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    test_actual_load(token, TEST_BIDDING_ZONE, start_date, end_date)
    test_generation_per_type(token, TEST_BIDDING_ZONE, start_date, end_date)
    test_day_ahead_forecast(token, TEST_BIDDING_ZONE, start_date, end_date)

def test_with_recent_data(token):
    """
    Test z niedawnymi danymi (ostatnie 7 dni)
    """
    logger.info("=== Testing with RECENT data (last week) ===")
    
    # Użyj dat z ostatniego tygodnia
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=7)
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    test_actual_load(token, TEST_BIDDING_ZONE, start_date, end_date)
    test_generation_per_type(token, TEST_BIDDING_ZONE, start_date, end_date)
    test_day_ahead_forecast(token, TEST_BIDDING_ZONE, start_date, end_date)

def test_with_future_data(token):
    """
    Test z przyszłymi danymi (prawdopodobnie nie będzie danych)
    """
    logger.info("=== Testing with FUTURE data (2025) ===")
    
    # Użyj dat z przyszłości (2025 rok)
    start_date = datetime(2025, 3, 1)
    end_date = datetime(2025, 5, 1)
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    test_actual_load(token, TEST_BIDDING_ZONE, start_date, end_date)
    test_generation_per_type(token, TEST_BIDDING_ZONE, start_date, end_date)
    test_day_ahead_forecast(token, TEST_BIDDING_ZONE, start_date, end_date)

def main():
    """
    Główna funkcja testowa
    """
    if len(sys.argv) < 2:
        logger.error("Please provide your ENTSO-E API token as command line argument")
        logger.error("Usage: python entsoe_api_test.py YOUR_API_TOKEN")
        return
    
    token = sys.argv[1]
    logger.info(f"Using token: {token[:5]}...{token[-5:]}")
    
    # Test z danymi historycznymi
    test_with_historical_data(token)
    
    time.sleep(2)  # Przerwa między testami
    
    # Test z niedawnymi danymi
    test_with_recent_data(token)
    
    time.sleep(2)  # Przerwa między testami
    
    # Test z przyszłymi danymi
    test_with_future_data(token)
    
    logger.info("All tests completed")

if __name__ == "__main__":
    main()