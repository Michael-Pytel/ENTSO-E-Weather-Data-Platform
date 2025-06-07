# test_historical_api.py - Testowanie API ENTSO-E z rzeczywistymi datami historycznymi
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import logging
import json
import xml.etree.ElementTree as ET
from config import CONFIG

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def ensure_temp_folder():
    """Zapewnienie istnienia folderu tymczasowego"""
    if not os.path.exists(CONFIG['temp_folder']):
        os.makedirs(CONFIG['temp_folder'])

def test_historical_api():
    """Test API ENTSO-E z datami historycznymi"""
    ensure_temp_folder()
    
    # Wybierz kraj do testów
    test_country = 'PL'  # Testujemy dla Polski
    
    # Konwersja kodu kraju jeśli potrzeba
    if test_country in CONFIG['country_map']:
        full_country_code = CONFIG['country_map'][test_country]
        logging.info(f"Zamieniono kod kraju na pełny format: {full_country_code}")
    else:
        full_country_code = test_country
    
    # Ustal RZECZYWISTE daty historyczne - przed rokiem
    now = datetime.now()
    end_date = datetime(now.year, now.month, 19)  # pierwszy dzień tego samego miesiąca rok temu
    start_date = end_date - timedelta(days=2)  # 2 dni wcześniej
    
    logging.info(f"Rozpoczynam testy API ENTSO-E dla kraju {test_country}")
    logging.info(f"Okres: {start_date} do {end_date} (rzeczywiste dane historyczne z roku {end_date.year})")
    
    # Format dat dla API - w UTC
    start_date_utc = pd.Timestamp(start_date).tz_localize('UTC')
    end_date_utc = pd.Timestamp(end_date).tz_localize('UTC')
    
    start_str = start_date_utc.strftime('%Y%m%d%H%M')
    end_str = end_date_utc.strftime('%Y%m%d%H%M')
    
    logging.info(f"Daty w formacie UTC: start={start_str}, end={end_str}")
    
    # Test 1: Pobieranie rzeczywistego obciążenia
    logging.info(f"Test 1: Pobieranie rzeczywistego obciążenia dla {full_country_code}")
    params_actual = {
        'securityToken': CONFIG['entsoe_api_key'],
        'documentType': 'A65',  # System total load
        'processType': 'A16',   # Realised (rzeczywiste)
        'outBiddingZone_Domain': full_country_code,
        'periodStart': start_str,
        'periodEnd': end_str
    }
    
    test_api_request("Rzeczywiste obciążenie", params_actual)
    
    # Test 2: Pobieranie prognozy obciążenia
    logging.info(f"Test 2: Pobieranie prognozy obciążenia dla {full_country_code}")
    params_forecast = {
        'securityToken': CONFIG['entsoe_api_key'],
        'documentType': 'A65',  # System total load
        'processType': 'A01',   # Day ahead (prognoza)
        'outBiddingZone_Domain': full_country_code,
        'periodStart': start_str,
        'periodEnd': end_str
    }
    
    test_api_request("Prognoza obciążenia", params_forecast)
    
    # Test 3: Alternatywne parametry dla prognozy obciążenia
    logging.info(f"Test 3: Alternatywne parametry dla prognozy obciążenia")
    params_alt = {
        'securityToken': CONFIG['entsoe_api_key'],
        'documentType': 'A65',  # System total load
        'processType': 'A31',   # Week ahead (prognoza)
        'outBiddingZone_Domain': full_country_code,
        'periodStart': start_str,
        'periodEnd': end_str
    }
    
    test_api_request("Alternatywna prognoza obciążenia", params_alt)
    
    logging.info("Testy zakończone")

def test_api_request(test_name, params):
    """Wykonanie zapytania do API ENTSO-E z logowaniem wyników"""
    api_url = CONFIG.get('api_endpoint', 'https://web-api.tp.entsoe.eu/api')
    
    logging.info(f"Test: {test_name}")
    logging.info(f"Parametry zapytania: {params}")
    
    try:
        response = requests.get(api_url, params=params, timeout=CONFIG['timeout'])
        logging.info(f"Kod odpowiedzi: {response.status_code}")
        
        # Zapisz odpowiedź do pliku
        filename = os.path.join(CONFIG['temp_folder'], f"test_{test_name.replace(' ', '_')}_{params['outBiddingZone_Domain']}_{params['periodStart']}.xml")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        if response.status_code == 200:
            logging.info(f"Zapytanie zakończone sukcesem! Odpowiedź zapisana do pliku: {filename}")
            
            # Analiza zawartości XML
            try:
                root = ET.fromstring(response.text)
                root_tag = root.tag.split('}')[-1] if '}' in root.tag else root.tag
                logging.info(f"Dokument XML: {root_tag}")
                
                if 'Acknowledgement_MarketDocument' in root_tag:
                    # To jest dokument błędu
                    reasons = root.findall('.//{*}Reason')
                    for reason in reasons:
                        code = reason.find('.//{*}code')
                        text = reason.find('.//{*}text')
                        if code is not None and text is not None:
                            logging.error(f"Błąd API: Kod {code.text}, Opis: {text.text}")
                else:
                    # To jest dokument z danymi
                    logging.info("Dokument zawiera dane!")
                    
                    # Próba zliczenia punktów danych
                    points = root.findall('.//{*}Point')
                    logging.info(f"Liczba punktów danych: {len(points)}")
                    
                    # Pokaż zakres czasowy
                    time_intervals = root.findall('.//{*}timeInterval')
                    if time_intervals:
                        for interval in time_intervals:
                            start = interval.find('.//{*}start')
                            end = interval.find('.//{*}end')
                            if start is not None and end is not None:
                                logging.info(f"Zakres czasowy: od {start.text} do {end.text}")
            except Exception as xml_error:
                logging.error(f"Błąd podczas analizy XML: {str(xml_error)}")
        else:
            # To jest błąd HTTP
            logging.error(f"Błąd HTTP: {response.status_code}")
            
            # Próba wyodrębnienia informacji o błędzie
            try:
                root = ET.fromstring(response.text)
                if 'Acknowledgement_MarketDocument' in root.tag:
                    reasons = root.findall('.//{*}Reason')
                    for reason in reasons:
                        code = reason.find('.//{*}code')
                        text = reason.find('.//{*}text')
                        if code is not None and text is not None:
                            logging.error(f"Błąd API: Kod {code.text}, Opis: {text.text}")
            except Exception as xml_error:
                logging.error(f"Nie udało się wyodrębnić błędu: {str(xml_error)}")
    
    except Exception as e:
        logging.error(f"Wyjątek podczas zapytania: {str(e)}")
    
    logging.info("-"*50)

if __name__ == "__main__":
    test_historical_api()