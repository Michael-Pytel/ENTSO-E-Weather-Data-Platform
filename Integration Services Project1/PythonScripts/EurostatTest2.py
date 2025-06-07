"""
EurostatDiagnostic.py
Narzędzie diagnostyczne dla endpointu energy_intensity w Eurostat API
"""

import requests
import json
import pandas as pd
import logging
from typing import Dict, List, Any
import traceback

# Konfiguracja logowania
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("eurostat_diagnostic.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

class EurostatDiagnostic:
    """Narzędzie diagnostyczne dla API Eurostat"""
    
    def __init__(self):
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
        self.test_countries = ['PL', 'DE', 'FR', 'ES', 'IT']
    
    def fetch_energy_intensity_data(self, params=None):
        """Pobiera dane o intensywności energetycznej z określonymi parametrami"""
        if params is None:
            params = {}
            
        default_params = {
            'format': 'JSON',
            'lang': 'EN',
            'geo': self.test_countries,
            'sinceTimePeriod': '2020'
        }
        
        all_params = default_params.copy()
        all_params.update(params)
        
        logger.info(f"Sending request to {self.base_url}/nrg_ind_ei with params: {all_params}")
        
        try:
            response = requests.get(f"{self.base_url}/nrg_ind_ei", params=all_params, timeout=60)
            
            if response.status_code != 200:
                logger.error(f"HTTP Error {response.status_code}: {response.text}")
                return None
            
            data = response.json()
            
            # Sprawdź, czy są dane w odpowiedzi
            if 'value' in data and data['value']:
                non_null_values = sum(1 for v in data['value'].values() if v is not None)
                logger.info(f"Success! Response contains {non_null_values} non-null values")
                return data
            else:
                logger.warning("Response contains no values")
                return data
                
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            traceback.print_exc()
            return None
    
    def analyze_response_structure(self, response_data):
        """Analizuje strukturę odpowiedzi API"""
        if not response_data:
            logger.error("No response data to analyze")
            return
        
        logger.info("\n=== ANALYSIS OF RESPONSE STRUCTURE ===")
        
        # 1. Główne sekcje odpowiedzi
        logger.info(f"Main sections: {list(response_data.keys())}")
        
        # 2. Wymiary (dimensions)
        if 'dimension' in response_data:
            logger.info(f"Dimensions: {list(response_data['dimension'].keys())}")
            
            # Analizuj każdy wymiar
            for dim_name, dim_data in response_data['dimension'].items():
                logger.info(f"\nDimension '{dim_name}':")
                if 'category' in dim_data:
                    logger.info(f"  Category keys: {list(dim_data['category'].keys())}")
                    
                    # Pokaż przykładowe wartości indeksów i etykiet
                    if 'index' in dim_data['category']:
                        indices = dim_data['category']['index']
                        logger.info(f"  Sample indices: {dict(list(indices.items())[:3])}...")
                    
                    if 'label' in dim_data['category']:
                        labels = dim_data['category']['label']
                        logger.info(f"  Sample labels: {dict(list(labels.items())[:3])}...")
        
        # 3. Wartości (values)
        if 'value' in response_data:
            values = response_data['value']
            non_null_count = sum(1 for v in values.values() if v is not None)
            total_count = len(values)
            
            logger.info(f"\nValues: {non_null_count} non-null out of {total_count} total")
            
            # Pokaż przykładowe klucze i wartości
            sample_values = dict(list(values.items())[:5])
            logger.info(f"Sample values: {sample_values}")
            
            # Analizuj strukturę kluczy
            sample_key = next(iter(values.keys()), None)
            if sample_key:
                logger.info(f"\nAnalyzing key structure. Sample key: '{sample_key}'")
                
                if ':' in sample_key:
                    parts = sample_key.split(':')
                    logger.info(f"Key contains ':' separator with {len(parts)} parts: {parts}")
                    
                    # Próba identyfikacji, co reprezentują poszczególne części
                    for i, part in enumerate(parts):
                        found = False
                        for dim_name, dim_data in response_data['dimension'].items():
                            if 'category' in dim_data and 'index' in dim_data['category']:
                                if part in dim_data['category']['index']:
                                    logger.info(f"  Part {i} ({part}) belongs to dimension '{dim_name}'")
                                    found = True
                                    break
                        if not found:
                            logger.info(f"  Part {i} ({part}) could not be identified with any dimension")
                else:
                    # Prawdopodobnie klucz numeryczny
                    try:
                        key_num = int(sample_key)
                        logger.info(f"Key is numeric: {key_num}")
                        
                        # Spróbuj rozszyfrować, jak działa indeksowanie
                        dimensions = list(response_data['dimension'].keys())
                        logger.info(f"Attempting to decode using dimensions: {dimensions}")
                        
                        # Sprawdź, czy możemy zdekodować numeryczne klucze
                        # To zwykle działa w formie: index = (index_dim1 * len_dim2 * len_dim3...) + (index_dim2 * len_dim3 * len_dim4...) + ...
                        logger.info("Checking if key can be decoded using common patterns...")
                        
                    except ValueError:
                        logger.info(f"Key appears to be non-numeric, non-separated: '{sample_key}'")
        
        logger.info("=== END OF ANALYSIS ===\n")
    
    def test_parsing_with_custom_function(self, response_data):
        """Testuje parsowanie odpowiedzi przy użyciu różnych funkcji"""
        if not response_data or 'value' not in response_data or 'dimension' not in response_data:
            logger.error("Invalid response data for parsing test")
            return []
        
        logger.info("\n=== TESTING PARSING FUNCTIONS ===")
        
        # Pobierz podstawowe wymiary
        geo_data = response_data['dimension'].get('geo', {}).get('category', {})
        time_data = response_data['dimension'].get('time', {}).get('category', {})
        
        geo_indices = geo_data.get('index', {})
        geo_labels = geo_data.get('label', {})
        time_indices = time_data.get('index', {})
        
        # 1. Testuj metodę parsowania z separatorami
        logger.info("Testing separator-based parsing method...")
        separator_records = self._test_separator_parsing(response_data['value'], response_data['dimension'], geo_indices, geo_labels, time_indices)
        logger.info(f"Separator method produced {len(separator_records)} records")
        
        # 2. Testuj metodę parsowania indeksów numerycznych
        logger.info("Testing numeric index parsing method...")
        numeric_records = self._test_numeric_parsing(response_data['value'], geo_indices, geo_labels, time_indices)
        logger.info(f"Numeric method produced {len(numeric_records)} records")
        
        # 3. Testuj metodę parsowania przez bezpośrednie mapowanie
        logger.info("Testing direct mapping method...")
        direct_records = self._test_direct_mapping(response_data['value'], response_data['dimension'], geo_indices, geo_labels, time_indices)
        logger.info(f"Direct mapping method produced {len(direct_records)} records")
        
        # Wybierz najlepszą metodę
        best_method = "separator"
        best_count = len(separator_records)
        
        if len(numeric_records) > best_count:
            best_method = "numeric"
            best_count = len(numeric_records)
            
        if len(direct_records) > best_count:
            best_method = "direct"
            best_count = len(direct_records)
        
        logger.info(f"Best parsing method: {best_method} with {best_count} records")
        
        # Pokaż przykładowe rekordy z najlepszej metody
        best_records = []
        if best_method == "separator":
            best_records = separator_records
        elif best_method == "numeric":
            best_records = numeric_records
        else:
            best_records = direct_records
            
        if best_records:
            logger.info("Sample records from best method:")
            for i, record in enumerate(best_records[:5]):
                logger.info(f"Record {i+1}: {record}")
        
        logger.info("=== END OF PARSING TESTS ===\n")
        
        # Implementacja metody, która zadziałała najlepiej
        logger.info("\n=== GENERATING OPTIMIZED PARSING FUNCTION ===")
        
        # Wygeneruj kod funkcji parsującej
        function_code = self._generate_parsing_function(best_method)
        logger.info(f"Generated parsing function for '{best_method}' method:\n{function_code}")
        
        return best_records
    
    def _test_separator_parsing(self, values, dimension, geo_indices, geo_labels, time_indices):
        """Testuje parsowanie z wykorzystaniem separatorów w kluczach"""
        records = []
        
        # Sprawdź, czy klucze używają separatora ":"
        sample_key = next(iter(values.keys()), None)
        if not sample_key or ":" not in sample_key:
            return records
            
        # Określ pozycje wymiarów w kluczu
        dim_positions = {}
        key_parts = sample_key.split(':')
        
        # Próba określenia, która pozycja odpowiada któremu wymiarowi
        for i, part in enumerate(key_parts):
            for dim_name, dim_data in dimension.items():
                if 'category' in dim_data and 'index' in dim_data['category']:
                    if part in dim_data['category']['index']:
                        dim_positions[dim_name] = i
                        break
        
        logger.debug(f"Separator parsing: Determined dimension positions: {dim_positions}")
        
        # Przetwarzaj wartości
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
        
        return records
    
    def _test_numeric_parsing(self, values, geo_indices, geo_labels, time_indices):
        """Testuje parsowanie z wykorzystaniem numerycznych kluczy"""
        records = []
        
        country_codes = list(geo_indices.keys())
        time_codes = list(time_indices.keys())
        
        if not country_codes or not time_codes:
            return records
            
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
            except (ValueError, IndexError) as e:
                continue
        
        return records
    
    def _test_direct_mapping(self, values, dimension, geo_indices, geo_labels, time_indices):
        """Testuje parsowanie przez bezpośrednie mapowanie wymiarów"""
        records = []
        
        # Iteruj po wszystkich krajach i latach
        for country_code in geo_indices.keys():
            for time_period in time_indices.keys():
                for key, value in values.items():
                    if value is None:
                        continue
                        
                    # Sprawdź, czy klucz odnosi się do tego kraju i okresu
                    matches_country = False
                    matches_time = False
                    
                    if ':' in key:
                        # Dla kluczy z separatorami
                        key_parts = key.split(':')
                        for part in key_parts:
                            if part == country_code:
                                matches_country = True
                            if part == time_period:
                                matches_time = True
                    else:
                        # Dla kluczy numerycznych (ale sprawdzamy alternatywnym sposobem)
                        # Ten sposób jest powolny, ale ma szansę działać dla różnych formatów kluczy
                        dimensions_list = list(dimension.keys())
                        for dim_name in dimensions_list:
                            dim_data = dimension[dim_name]
                            if 'category' in dim_data and 'index' in dim_data['category']:
                                if dim_name == 'geo' and country_code in dim_data['category']['index']:
                                    matches_country = True
                                if dim_name == 'time' and time_period in dim_data['category']['index']:
                                    matches_time = True
                    
                    if matches_country and matches_time:
                        record = {
                            'country_code': country_code,
                            'country_name': geo_labels.get(country_code, country_code),
                            'time_period': time_period,
                            'value': float(value) if value is not None else None
                        }
                        records.append(record)
                        break  # Znaleziono wartość dla tej kombinacji kraju i czasu
        
        return records
    
    def _generate_parsing_function(self, method):
        """Generuje kod funkcji parsującej na podstawie najlepszej metody"""
        if method == "separator":
            return """
def _parse_energy_intensity_data(self, values, geo_indices, geo_labels, time_indices, dimension):
    records = []
    
    # Określ pozycje wymiarów w kluczu
    sample_key = next(iter(values.keys()), None)
    if not sample_key or ":" not in sample_key:
        return records
        
    dim_positions = {}
    key_parts = sample_key.split(':')
    
    # Próba określenia, która pozycja odpowiada któremu wymiarowi
    for i, part in enumerate(key_parts):
        for dim_name, dim_data in dimension.items():
            if 'category' in dim_data and 'index' in dim_data['category']:
                if part in dim_data['category']['index']:
                    dim_positions[dim_name] = i
                    break
    
    # Przetwarzaj wartości
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
    
    return records
"""
        elif method == "numeric":
            return """
def _parse_energy_intensity_data(self, values, geo_indices, geo_labels, time_indices, dimension):
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
"""
        else:  # direct mapping
            return """
def _parse_energy_intensity_data(self, values, geo_indices, geo_labels, time_indices, dimension):
    records = []
    
    # Najpierw spróbuj metody z separatorami
    sample_key = next(iter(values.keys()), None)
    if sample_key and ":" in sample_key:
        dim_positions = {}
        key_parts = sample_key.split(':')
        
        # Próba określenia, która pozycja odpowiada któremu wymiarowi
        for i, part in enumerate(key_parts):
            for dim_name, dim_data in dimension.items():
                if 'category' in dim_data and 'index' in dim_data['category']:
                    if part in dim_data['category']['index']:
                        dim_positions[dim_name] = i
                        break
        
        # Przetwarzaj wartości z separatorami
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
    else:
        # Spróbuj metody numerycznej
        try:
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
        except Exception:
            pass
    
    # Jeśli poprzednie metody nie dały wyników, użyj metody bezpośredniego mapowania
    if not records:
        # Iteruj po wszystkich krajach i latach
        for country_code in geo_indices.keys():
            for time_period in time_indices.keys():
                for key, value in values.items():
                    if value is None:
                        continue
                        
                    # Sprawdź, czy klucz odnosi się do tego kraju i okresu
                    matches_country = False
                    matches_time = False
                    
                    if ':' in key:
                        # Dla kluczy z separatorami
                        key_parts = key.split(':')
                        for part in key_parts:
                            if part == country_code:
                                matches_country = True
                            if part == time_period:
                                matches_time = True
                    
                    if matches_country and matches_time:
                        record = {
                            'country_code': country_code,
                            'country_name': geo_labels.get(country_code, country_code),
                            'time_period': time_period,
                            'value': float(value) if value is not None else None
                        }
                        records.append(record)
                        break  # Znaleziono wartość dla tej kombinacji kraju i czasu
    
    return records
"""
    
    def test_energy_intensity_endpoints(self):
        """Testuje różne kombinacje parametrów dla endpointu energy_intensity"""
        logger.info("\n=== TESTING ENERGY INTENSITY ENDPOINT WITH DIFFERENT PARAMETERS ===")
        
        test_cases = [
            {"description": "No parameters", "params": {}},
            {"description": "With unit=KGOE_TEUR", "params": {"unit": "KGOE_TEUR"}},
            {"description": "With nrg_bal=EI_GDP_PPS", "params": {"nrg_bal": "EI_GDP_PPS"}},
            {"description": "With freq=A", "params": {"freq": "A"}},
            {"description": "With unit=KGOE_TEUR_PPS", "params": {"unit": "KGOE_TEUR_PPS"}},
            {"description": "With nrg_bal=EI_GDP_CLV05", "params": {"nrg_bal": "EI_GDP_CLV05"}},
            {"description": "With nrg_bal=EI_GDP_PPS and unit=KGOE_TEUR", "params": {"nrg_bal": "EI_GDP_PPS", "unit": "KGOE_TEUR"}},
        ]
        
        results = []
        
        for tc in test_cases:
            logger.info(f"\nTesting: {tc['description']}")
            response = self.fetch_energy_intensity_data(tc['params'])
            
            success = False
            value_count = 0
            
            if response and 'value' in response:
                value_count = sum(1 for v in response['value'].values() if v is not None)
                success = value_count > 0
            
            result = {
                "description": tc['description'],
                "params": tc['params'],
                "success": success,
                "value_count": value_count
            }
            
            results.append(result)
            
            if success and value_count > 0:
                # Dla udanych testów analizujemy strukturę i testujemy parsowanie
                logger.info(f"Found successful configuration with {value_count} values, analyzing structure...")
                self.analyze_response_structure(response)
                
                if value_count > 10:  # Jeśli jest wystarczająco dużo danych, testujemy parsowanie
                    self.test_parsing_with_custom_function(response)
        
        # Podsumowanie wyników
        logger.info("\n=== SUMMARY OF ENDPOINT TESTS ===")
        results.sort(key=lambda x: x['value_count'], reverse=True)
        
        for i, r in enumerate(results):
            status = "✅" if r['success'] else "❌"
            params_str = ", ".join([f"{k}={v}" for k, v in r['params'].items()]) if r['params'] else "bez parametrów"
            logger.info(f"{i+1}. {status} {params_str}: {r['value_count']} wartości")
        
        if results and results[0]['success']:
            best_result = results[0]
            logger.info(f"\nNajlepsza konfiguracja: {best_result['description']} z {best_result['value_count']} wartościami")
            
            logger.info(f"""
'energy_intensity': {{
    'code': 'nrg_ind_ei',
    'description': 'Energy intensity of the economy',
    'params': {{
        {', '.join([f"'{k}': '{v}'" for k, v in best_result['params'].items()])}
    }}
}}
""")
        
        logger.info("=== END OF ENDPOINT TESTS ===")
    
    def run_complete_diagnostics(self):
        """Uruchamia pełną diagnostykę"""
        logger.info("Starting complete diagnostics for Eurostat energy_intensity endpoint")
        
        # 1. Testuj endpoint z różnymi parametrami
        self.test_energy_intensity_endpoints()
        
        # 2. Testowanie implementacji w kliencie Eurostat
        logger.info("\n=== TESTING IMPLEMENTATION IN EUROSTAT CLIENT ===")
        
        from EurostatClient import EurostatClient
        
        try:
            client = EurostatClient()
            logger.info("Testing get_energy_intensity_data method with diagnostic logging")
            
            # Dodajemy diagnostyczne logowanie
            logging.getLogger('__main__').setLevel(logging.DEBUG)
            
            # Pobieramy dane z domyślnymi parametrami (bez parametrów)
            df = client.get_energy_intensity_data(['PL', 'DE', 'FR', 'ES', 'IT'], 2020)
            
            if df.empty:
                logger.error("get_energy_intensity_data returned empty DataFrame")
            else:
                logger.info(f"get_energy_intensity_data returned DataFrame with {len(df)} rows")
                logger.info(f"DataFrame columns: {df.columns.tolist()}")
                logger.info(f"DataFrame sample:\n{df.head()}")
                
                # Sprawdź, czy są jakieś niepuste wartości
                non_null_count = df['value'].notnull().sum()
                logger.info(f"Non-null values in 'value' column: {non_null_count} out of {len(df)}")
                
                if non_null_count > 0:
                    logger.info("Implementation appears to be working correctly!")
                else:
                    logger.warning("DataFrame contains rows, but all values are null")
            
        except Exception as e:
            logger.error(f"Error testing EurostatClient implementation: {str(e)}")
            traceback.print_exc()
        
        logger.info("=== END OF IMPLEMENTATION TEST ===")
        
        logger.info("\nDiagnostics completed. Check the logs for detailed results and recommendations.")


if __name__ == "__main__":
    diagnostic = EurostatDiagnostic()
    diagnostic.run_complete_diagnostics()