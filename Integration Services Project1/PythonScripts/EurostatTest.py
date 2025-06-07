"""
EurostatTester2.py
Skrypt testowy do sprawdzenia nowych alternatywnych datasetów dla problematycznych kategorii
"""

import requests
import logging
import time
import json
import sys
from typing import Dict, List, Optional, Any

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EurostatTester2")

class EurostatDatasetTester:
    """Tester dla konkretnych problematycznych datasetów"""
    
    def __init__(self):
        """Inicjalizacja testera"""
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
        
        # Domyślne parametry
        self.default_params = {
            'format': 'JSON',
            'lang': 'EN'
        }
        
        # Testowe kraje
        self.test_countries = ['PL', 'DE', 'FR', 'ES', 'IT']
        
        # Problem 1: Household Size
        self.household_datasets = [
            {
                'code': 'ilc_lvph01',
                'description': 'Average household size',
                'params': [
                    {'age': 'TOTAL', 'hhtyp': 'TOTAL', 'incgrp': 'TOTAL', 'tenure': 'TOTAL'},
                    {'age': 'TOTAL', 'hhtyp': 'TOTAL'},
                    {'hhtyp': 'TOTAL'},
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'ilc_lvph03',
                'description': 'Distribution of households by household type',
                'params': [
                    {'hhtyp': 'TOTAL'},
                    {'hhtyp': 'A1'}, # Single person
                    {'hhtyp': 'A1_DCH'}, # Single person with dependent children
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'hbs_struc',
                'description': 'Structure of consumption expenditure by household',
                'params': [
                    {'coicop': 'TOTAL', 'unit': 'PC'},
                    {'coicop': 'CP00', 'unit': 'PC'},
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'demo_pjangroup',
                'description': 'Population by sex, age, country of birth',
                'params': [
                    {'sex': 'T', 'age': 'TOTAL', 'c_birth': 'TOTAL'},
                    {'sex': 'T', 'age': 'TOTAL'},
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'lfst_hhnhtych',
                'description': 'Number of persons by household type',
                'params': [
                    {'hhtyp': 'TOTAL', 'unit': 'THS_PER'},
                    {'hhtyp': 'TOTAL', 'unit': 'PC'},
                    {'hhtyp': 'A1', 'unit': 'THS_PER'},
                    {'unit': 'THS_PER'},
                    {'unit': 'PC'},
                    {}  # Bez parametrów
                ]
            }
        ]
        
        # Problem 2: Heating Systems
        self.heating_datasets = [
            {
                'code': 'nrg_d_hhq',
                'description': 'Primary heating systems',
                'params': [
                    {'unit': 'THS_T', 'nrg_bal': 'FC_OTH_HH_E'},
                    {'unit': 'PC', 'nrg_bal': 'FC_OTH_HH_E'},
                    {'unit': 'THS_T', 'nrg_bal': 'FC_OBS_HH_E'},
                    {'unit': 'PC', 'siec': 'TOTAL'},
                    {'siec': 'TOTAL'},
                    {'nrg_bal': 'FC_OTH_HH_E'},
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'nrg_bal_c',
                'description': 'Complete energy balances',
                'params': [
                    {'unit': 'TJ', 'siec': 'TOTAL', 'nrg_bal': 'FC_OTH_HH_E'},
                    {'unit': 'KTOE', 'siec': 'TOTAL', 'nrg_bal': 'FC_OTH_HH_E'},
                    {'unit': 'TJ', 'siec': 'G3000', 'nrg_bal': 'FC_OTH_HH_E'}, # Natural gas
                    {'unit': 'TJ', 'siec': 'O4000XBIO', 'nrg_bal': 'FC_OTH_HH_E'}, # Oil
                    {'unit': 'TJ', 'siec': 'S2000', 'nrg_bal': 'FC_OTH_HH_E'}, # Coal
                    {'unit': 'TJ', 'siec': 'E7000', 'nrg_bal': 'FC_OTH_HH_E'}, # Electricity
                    {'unit': 'TJ', 'siec': 'RA000', 'nrg_bal': 'FC_OTH_HH_E'}, # Heat
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'nrg_d_hhq_peh',
                'description': 'Penetration rates of heating elements',
                'params': [
                    {'unit': 'PC'},
                    {'unit': 'PC', 'n_prod': 'HE_SH'},  # Space heating
                    {'unit': 'PC', 'n_prod': 'HE_WH'},  # Water heating
                    {'n_prod': 'HE_SH'},
                    {}  # Bez parametrów
                ]
            },
            {
                'code': 'nrg_pc_202',
                'description': 'Gas prices for household consumers',
                'params': [
                    {'unit': 'KWH', 'product': '4100', 'tax': 'X_TAX', 'currency': 'EUR'},
                    {'unit': 'KWH', 'product': '4100', 'tax': 'X_TAX'},
                    {'unit': 'KWH', 'product': '4100'},
                    {}  # Bez parametrów
                ]
            }
        ]
    
    def test_dataset(self, dataset_info: Dict, params_set: Dict, countries: List[str] = None) -> Dict:
        """
        Testuje konkretny dataset z określonymi parametrami
        
        Args:
            dataset_info: Informacje o datasecie
            params_set: Zestaw parametrów do przetestowania
            countries: Lista krajów do przetestowania
            
        Returns:
            Słownik z wynikami testu
        """
        dataset_code = dataset_info['code']
        url = f"{self.base_url}/{dataset_code}"
        
        # Przygotuj parametry
        all_params = self.default_params.copy()
        all_params.update(params_set)
        
        if countries:
            all_params['geo'] = countries
        else:
            all_params['geo'] = self.test_countries
        
        all_params['sinceTimePeriod'] = '2020'
        
        try:
            logger.info(f"Testing: {dataset_code} with params: {json.dumps(params_set)}")
            logger.info(f"Full params: {all_params}")
            
            response = requests.get(url, params=all_params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'value' in data and data['value']:
                    records_count = len(data['value'])
                    logger.info(f"SUCCESS: Received {records_count} records")
                    
                    # Pokaż przykładowe wartości
                    if records_count > 0:
                        sample_values = list(data['value'].items())[:3]
                        logger.info(f"Sample values: {sample_values}")
                        
                        # Wypisz wymiary
                        if 'dimension' in data:
                            dimensions = list(data['dimension'].keys())
                            logger.info(f"Dimensions: {dimensions}")
                    
                    return {
                        'status': 'SUCCESS',
                        'records': records_count,
                        'message': f"Received {records_count} records",
                        'data': data if records_count > 0 else None
                    }
                else:
                    logger.warning(f"EMPTY: Response contains no values")
                    return {
                        'status': 'EMPTY',
                        'records': 0,
                        'message': "Response contains no values"
                    }
            else:
                logger.error(f"ERROR: HTTP {response.status_code}: {response.text}")
                return {
                    'status': 'ERROR',
                    'records': 0,
                    'message': f"HTTP Error {response.status_code}: {response.text}"
                }
                
        except Exception as e:
            logger.error(f"EXCEPTION: {str(e)}")
            return {
                'status': 'EXCEPTION',
                'records': 0,
                'message': str(e)
            }
    
    def test_household_datasets(self) -> Dict[str, Dict]:
        """Testuje wszystkie datasety związane z wielkością gospodarstw domowych"""
        results = {}
        
        for dataset in self.household_datasets:
            dataset_code = dataset['code']
            dataset_results = {}
            
            for i, params in enumerate(dataset['params']):
                time.sleep(1)  # Unikaj przekroczenia limitu zapytań
                test_name = f"params_set_{i+1}"
                dataset_results[test_name] = self.test_dataset(dataset, params)
            
            results[dataset_code] = dataset_results
        
        return results
    
    def test_heating_datasets(self) -> Dict[str, Dict]:
        """Testuje wszystkie datasety związane z systemami grzewczymi"""
        results = {}
        
        for dataset in self.heating_datasets:
            dataset_code = dataset['code']
            dataset_results = {}
            
            for i, params in enumerate(dataset['params']):
                time.sleep(1)  # Unikaj przekroczenia limitu zapytań
                test_name = f"params_set_{i+1}"
                dataset_results[test_name] = self.test_dataset(dataset, params)
            
            results[dataset_code] = dataset_results
        
        return results
    
    def test_db_schema(self) -> None:
        """Testuje strukturę bazy danych i sugeruje zmiany"""
        logger.info("Diagnostic info for database schema issue:")
        logger.info("Error was: Invalid column name 'urbanization_rate', 'service_sector_percentage', etc.")
        logger.info("This suggests the staging_eurostat_integrated table is missing the following columns:")
        
        missing_columns = [
            "urbanization_rate FLOAT",
            "service_sector_percentage FLOAT",
            "industry_sector_percentage FLOAT",
            "avg_household_size FLOAT",
            "energy_poverty_rate FLOAT", 
            "primary_heating_type NVARCHAR(50)"
        ]
        
        # Generowanie SQL do aktualizacji tabeli
        sql_update = "ALTER TABLE staging_eurostat_integrated ADD\n"
        sql_update += ",\n".join(missing_columns)
        sql_update += ";"
        
        logger.info("\nSuggested SQL to fix the database schema:")
        logger.info(sql_update)
        
        # Alternatywnie, można dodać opcję dla starszego schematu
        logger.info("\nOr alternatively, modify the code to work with existing schema:")
        logger.info("Add a check in save_to_staging() to detect existing columns")
        logger.info("and only save data for columns that exist in the table.")
    
    def run_all_tests(self) -> None:
        """Uruchamia wszystkie testy i wyświetla podsumowanie"""
        logger.info("Starting Eurostat dataset tests for problematic categories")
        logger.info("=" * 50)
        
        logger.info("\n\n--- TESTING HOUSEHOLD SIZE DATASETS ---\n")
        household_results = self.test_household_datasets()
        
        logger.info("\n\n--- TESTING HEATING SYSTEMS DATASETS ---\n")
        heating_results = self.test_heating_datasets()
        
        logger.info("\n\n--- DATABASE SCHEMA DIAGNOSTIC ---\n")
        self.test_db_schema()
        
        # Podsumowanie wyników
        logger.info("\n\n--- SUMMARY OF RESULTS ---\n")
        
        logger.info("HOUSEHOLD SIZE DATASETS:")
        self._print_summary(household_results)
        
        logger.info("\nHEATING SYSTEMS DATASETS:")
        self._print_summary(heating_results)
        
        # Rekomendacje
        successful_household = self._get_successful_configs(household_results)
        successful_heating = self._get_successful_configs(heating_results)
        
        logger.info("\n\n--- RECOMMENDED CONFIGURATIONS ---\n")
        
        if successful_household:
            logger.info("For household_size dataset, use:")
            for config in successful_household[:2]:  # Tylko 2 najlepsze
                logger.info(f"  {config['code']} with params: {json.dumps(config['params'])}")
        else:
            logger.info("No successful configurations found for household_size")
        
        if successful_heating:
            logger.info("\nFor heating_systems dataset, use:")
            for config in successful_heating[:2]:  # Tylko 2 najlepsze
                logger.info(f"  {config['code']} with params: {json.dumps(config['params'])}")
        else:
            logger.info("No successful configurations found for heating_systems")
    
    def _print_summary(self, results: Dict[str, Dict]) -> None:
        """Wyświetla podsumowanie wyników testów"""
        for dataset_code, dataset_results in results.items():
            logger.info(f"Dataset: {dataset_code}")
            
            for test_name, test_result in dataset_results.items():
                status = test_result['status']
                records = test_result['records']
                message = test_result.get('message', '')
                
                status_icon = {
                    'SUCCESS': '✓',
                    'EMPTY': '⚠',
                    'ERROR': '✗',
                    'EXCEPTION': '✗'
                }.get(status, '?')
                
                logger.info(f"  {status_icon} {test_name}: {status} - {records} records - {message}")
    
    def _get_successful_configs(self, results: Dict[str, Dict]) -> List[Dict]:
        """Zwraca listę udanych konfiguracji, posortowaną wg liczby rekordów"""
        successful = []
        
        for dataset_code, dataset_results in results.items():
            for test_name, test_result in dataset_results.items():
                if test_result['status'] == 'SUCCESS' and test_result['records'] > 0:
                    # Znajdź numer zestawu parametrów
                    param_index = int(test_name.split('_')[-1]) - 1
                    
                    # Znajdź dataset i parametry
                    dataset_list = self.household_datasets if dataset_code in [d['code'] for d in self.household_datasets] else self.heating_datasets
                    dataset_info = next((d for d in dataset_list if d['code'] == dataset_code), None)
                    
                    if dataset_info and param_index < len(dataset_info['params']):
                        successful.append({
                            'code': dataset_code,
                            'params': dataset_info['params'][param_index],
                            'records': test_result['records']
                        })
        
        # Sortuj wg liczby rekordów, malejąco
        successful.sort(key=lambda x: x['records'], reverse=True)
        return successful


def main():
    """Główna funkcja skryptu"""
    tester = EurostatDatasetTester()
    tester.run_all_tests()
    
    logger.info("\n\nTESTS COMPLETED. Use the recommended configurations to update your EurostatClient.py")


if __name__ == "__main__":
    main()