"""
test_eurostat_fixed.py
Naprawiony test API Eurostat z poprawnymi parametrami
"""

import requests
import json
import time
import logging
from pprint import pprint

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('eurostat_test')

def test_eurostat_api(dataset_code, params=None, print_full_response=False, save_to_file=False):
    """Testuje zapytanie do API Eurostat"""
    if params is None:
        params = {}
    
    # Dodaj podstawowe parametry
    if 'format' not in params:
        params['format'] = 'JSON'
    if 'lang' not in params:
        params['lang'] = 'EN'
    
    base_url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}"
    
    logger.info(f"Testowanie API Eurostat dla datasetu: {dataset_code}")
    logger.info(f"URL: {base_url}")
    logger.info(f"Parametry: {params}")
    
    try:
        response = requests.get(base_url, params=params, timeout=60)
        
        logger.info(f"Kod odpowiedzi: {response.status_code}")
        logger.info(f"Pełny URL zapytania: {response.url}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Zapisz do pliku jeśli potrzeba
            if save_to_file:
                filename = f"{dataset_code}_test_response.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"Odpowiedź zapisana do pliku: {filename}")
            
            # Analiza odpowiedzi
            if 'dimension' in data:
                dims = list(data['dimension'].keys())
                logger.info(f"Wymiary w odpowiedzi: {dims}")
                
                # Sprawdź liczby rekordów
                values_count = len(data.get('value', {}))
                logger.info(f"Liczba wartości: {values_count}")
                
                # Analiza wymiarów
                for dim_name, dim_data in data['dimension'].items():
                    if 'category' in dim_data and 'index' in dim_data['category']:
                        categories = dim_data['category']['index']
                        logger.info(f"Wymiar {dim_name}: {list(categories.keys())[:10]}")  # Pierwsze 10
                
                # Sprawdź czy są jakieś wartości
                if values_count > 0:
                    sample_values = list(data['value'].items())[:5]
                    logger.info(f"Przykładowe wartości: {sample_values}")
                else:
                    logger.warning("BRAK WARTOŚCI W ODPOWIEDZI!")
                    
                    # Sprawdź czy nie ma błędnych parametrów
                    if 'extension' in data and 'positions-with-no-data' in data['extension']:
                        no_data_positions = data['extension']['positions-with-no-data']
                        logger.warning(f"Pozycje bez danych: {no_data_positions}")
                
                if print_full_response:
                    logger.info("Pełna odpowiedź:")
                    print(json.dumps(data, indent=2))
            
            return data
            
        else:
            logger.error(f"Błąd: {response.status_code}")
            try:
                error_data = response.json()
                logger.error(f"Szczegóły błędu: {error_data}")
            except:
                logger.error(f"Treść odpowiedzi: {response.text}")
            
            return None
            
    except Exception as e:
        logger.error(f"Wyjątek podczas zapytania: {str(e)}")
        return None

def test_available_units_for_dataset(dataset_code, basic_params):
    """Test dostępnych jednostek dla datasetu"""
    logger.info(f"\n=== TESTOWANIE DOSTĘPNYCH JEDNOSTEK DLA {dataset_code} ===")
    
    # Podstawowe zapytanie bez unit
    base_params = basic_params.copy()
    if 'unit' in base_params:
        del base_params['unit']
    
    # Dodaj tylko jeden kraj dla testów
    base_params['geo'] = 'DE'
    base_params['time'] = '2023'
    
    response = test_eurostat_api(dataset_code, base_params)
    
    if response and 'dimension' in response:
        unit_dimension = response['dimension'].get('unit', {})
        if 'category' in unit_dimension:
            available_units = unit_dimension['category'].get('index', {})
            logger.info(f"Dostępne jednostki dla {dataset_code}: {list(available_units.keys())}")
            return list(available_units.keys())
    
    return []

def print_section_separator():
    """Drukuje separator sekcji"""
    print("\n" + "="*70 + "\n")

def main():
    """Funkcja główna testująca wszystkie datasety z poprawnymi parametrami"""
    
    # Lista testowanych krajów
    test_countries = ['PL', 'DE', 'FR']
    
    print_section_separator()
    logger.info("TEST 1: DANE POPULACJI (demo_pjan)")
    params = {
        'sex': 'T',
        'age': 'TOTAL',
        'geo': test_countries,
        'sinceTimePeriod': '2020'
    }
    population_data = test_eurostat_api('demo_pjan', params, save_to_file=True)
    
    print_section_separator()
    logger.info("TEST 2: PKB PER CAPITA (nama_10_pc) - Test jednostek")
    
    # Najpierw sprawdź dostępne jednostki
    available_units = test_available_units_for_dataset('nama_10_pc', {
        'na_item': 'B1GQ',
        'geo': test_countries,
        'sinceTimePeriod': '2020'
    })
    
    # Testuj różne jednostki
    units_to_test = ['CP_EUR_HAB', 'CP_PPS_EU27_2020_HAB', 'CLV_PCH_PRE_HAB', 'PD_PCH_PRE_HAB']
    
    for unit in units_to_test:
        logger.info(f"Testowanie PKB z jednostką: {unit}")
        params = {
            'unit': unit,
            'na_item': 'B1GQ',
            'geo': test_countries,
            'sinceTimePeriod': '2020'
        }
        gdp_data = test_eurostat_api('nama_10_pc', params, save_to_file=False)
        
        if gdp_data and gdp_data.get('value'):
            logger.info(f"SUKCES! Jednostka {unit} zwraca dane")
            # Zapisz udaną odpowiedź
            with open(f'nama_10_pc_success_{unit}.json', 'w', encoding='utf-8') as f:
                json.dump(gdp_data, f, indent=2, ensure_ascii=False)
            break
        else:
            logger.warning(f"Jednostka {unit} nie zwraca danych")
    
    print_section_separator()
    logger.info("TEST 3: CENY ENERGII ELEKTRYCZNEJ (nrg_pc_204)")
    params = {
        'unit': 'KWH',
        'product': '6000',
        'nrg_cons': 'KWH2500-4999',
        'tax': 'X_TAX',
        'currency': 'EUR',
        'geo': test_countries,
        'sinceTimePeriod': '2020'
    }
    electricity_data = test_eurostat_api('nrg_pc_204', params, save_to_file=True)
    
    print_section_separator()
    logger.info("TEST 4: INTENSYWNOŚĆ ENERGETYCZNA (sdg_07_30) - Test jednostek")
    
    # Test dostępnych jednostek
    available_units = test_available_units_for_dataset('sdg_07_30', {
        'geo': test_countries,
        'sinceTimePeriod': '2020'
    })
    
    # Testuj różne jednostki
    units_to_test = ['KGOE_TEUR', 'KGOE_EUR', 'TOE_TEUR', 'I05', 'I10', 'I15']
    
    for unit in units_to_test:
        logger.info(f"Testowanie intensywności energetycznej z jednostką: {unit}")
        params = {
            'unit': unit,
            'geo': test_countries,
            'sinceTimePeriod': '2020'
        }
        energy_data = test_eurostat_api('sdg_07_30', params, save_to_file=False)
        
        if energy_data and energy_data.get('value'):
            logger.info(f"SUKCES! Jednostka {unit} zwraca dane")
            with open(f'sdg_07_30_success_{unit}.json', 'w', encoding='utf-8') as f:
                json.dump(energy_data, f, indent=2, ensure_ascii=False)
            break
        else:
            logger.warning(f"Jednostka {unit} nie zwraca danych")
    
    print_section_separator()
    logger.info("TEST 5: STOPA BEZROBOCIA (une_rt_a) - Test parametrów")
    
    # Test dostępnych jednostek/parametrów
    available_units = test_available_units_for_dataset('une_rt_a', {
        'sex': 'T',
        'age': 'TOTAL',
        'geo': test_countries,
        'sinceTimePeriod': '2020'
    })
    
    # Testuj różne jednostki
    units_to_test = ['PC_ACT', 'PC_POP', 'THS_PER', 'PC_LAB']
    
    for unit in units_to_test:
        logger.info(f"Testowanie bezrobocia z jednostką: {unit}")
        params = {
            'sex': 'T',
            'age': 'TOTAL',
            'unit': unit,
            'geo': test_countries,
            'sinceTimePeriod': '2020'
        }
        unemployment_data = test_eurostat_api('une_rt_a', params, save_to_file=False)
        
        if unemployment_data and unemployment_data.get('value'):
            logger.info(f"SUKCES! Jednostka {unit} zwraca dane")
            with open(f'une_rt_a_success_{unit}.json', 'w', encoding='utf-8') as f:
                json.dump(unemployment_data, f, indent=2, ensure_ascii=False)
            break
        else:
            logger.warning(f"Jednostka {unit} nie zwraca danych")
    
    print_section_separator()
    logger.info("TEST DODATKOWY: Sprawdzenie z pojedynczym krajem i rokiem")
    
    # Test z minimalnymi parametrami
    datasets_to_test = [
        ('nama_10_pc', {'na_item': 'B1GQ', 'geo': 'DE', 'time': '2023'}),
        ('sdg_07_30', {'geo': 'DE', 'time': '2023'}),
        ('une_rt_a', {'sex': 'T', 'age': 'TOTAL', 'geo': 'DE', 'time': '2023'})
    ]
    
    for dataset_code, params in datasets_to_test:
        logger.info(f"Test minimalny dla {dataset_code}")
        result = test_eurostat_api(dataset_code, params, save_to_file=False)
        
        if result and result.get('value'):
            logger.info(f"SUKCES! {dataset_code} zwraca dane z minimalnymi parametrami")
        else:
            logger.warning(f"BRAK DANYCH dla {dataset_code} nawet z minimalnymi parametrami")
    
    print_section_separator()
    logger.info("WSZYSTKIE TESTY ZAKOŃCZONE")

if __name__ == "__main__":
    main()