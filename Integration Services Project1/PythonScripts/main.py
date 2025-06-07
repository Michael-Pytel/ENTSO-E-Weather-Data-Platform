# main.py - Główny skrypt ETL
import sys
import os
import subprocess
from datetime import datetime, timedelta
import time
import logging
from utils import setup_logging, check_sql_tables
from config import CONFIG

def run_script(script_name, params=None):
    """Uruchamia skrypt Python z podanymi parametrami"""
    cmd = [sys.executable, script_name]
    if params:
        cmd.extend(params)
    
    logging.info(f"Uruchamianie: {' '.join(cmd)}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if stdout:
        logging.info("STDOUT:")
        for line in stdout.splitlines():
            logging.info(line)
    
    if stderr:
        logging.error("STDERR:")
        for line in stderr.splitlines():
            logging.error(line)
    
    return process.returncode

def process_historical_data():
    """Pobieranie historycznych danych ENTSO-E dla wszystkich krajów"""
    logging.info("=== Rozpoczynam pobieranie danych historycznych ===")
    
    # Pobieranie historycznych danych o obciążeniu
    return_code = run_script('entsoe_loader.py', ['load', '--historical'])
    if return_code != 0:
        logging.error("Błąd podczas pobierania historycznych danych o obciążeniu")
        return return_code
    
    # Pobieranie historycznych danych o cenach
    return_code = run_script('entsoe_loader.py', ['price', '--historical'])
    if return_code != 0:
        logging.error("Błąd podczas pobierania historycznych danych o cenach")
        return return_code
    
    # Pobieranie historycznych danych o generacji
    return_code = run_script('entsoe_loader.py', ['generation', '--historical'])
    if return_code != 0:
        logging.error("Błąd podczas pobierania historycznych danych o generacji")
        return return_code
    
    logging.info("=== Zakończono pobieranie danych historycznych ===")
    return 0

def process_daily_data():
    """Pobieranie przyrostowych danych ENTSO-E dla wszystkich krajów"""
    logging.info("=== Rozpoczynam pobieranie danych przyrostowych ===")
    
    # Ustalenie dat dla ETL
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Pobieranie danych o obciążeniu
    return_code = run_script('entsoe_loader.py', ['load', '--start', start_date, '--end', end_date])
    if return_code != 0:
        logging.error("Błąd podczas pobierania przyrostowych danych o obciążeniu")
        return return_code
    
    # Pobieranie danych o cenach
    return_code = run_script('entsoe_loader.py', ['price', '--start', start_date, '--end', end_date])
    if return_code != 0:
        logging.error("Błąd podczas pobierania przyrostowych danych o cenach")
        return return_code
    
    # Pobieranie danych o generacji
    return_code = run_script('entsoe_loader.py', ['generation', '--start', start_date, '--end', end_date])
    if return_code != 0:
        logging.error("Błąd podczas pobierania przyrostowych danych o generacji")
        return return_code
    
    # Etap 2: Pobieranie danych pogodowych
    logging.info("=== Etap 2: Pobieranie danych pogodowych ===")
    return_code = run_script('weather_loader.py', [start_date, end_date])
    if return_code != 0:
        logging.error("Błąd podczas pobierania danych pogodowych")
        return return_code
    
    # Etap 3: Integracja danych
    logging.info("=== Etap 3: Integracja danych ===")
    return_code = run_script('data_integration.py')
    if return_code != 0:
        logging.error("Błąd podczas integracji danych")
        return return_code
    
    logging.info("=== Zakończono pobieranie danych przyrostowych ===")
    return 0

def main():
    """Główna funkcja ETL"""
    # Konfiguracja logowania
    setup_logging('main_etl')
    
    start_time = time.time()
    logging.info(f"Rozpoczęcie procesu ETL: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(scripts_dir)
    
    # Sprawdzenie czy istnieją tabele w bazie danych
    missing_tables = check_sql_tables()
    
    # Sprawdzenie argumentów wiersza poleceń
    if len(sys.argv) > 1 and sys.argv[1] == '--historical':
        # Uruchomienie pobierania danych historycznych
        logging.info("Tryb: pobieranie danych historycznych")
        return_code = process_historical_data()
    else:
        # Standardowe uruchomienie (przyrostowe)
        logging.info("Tryb: pobieranie danych przyrostowych")
        return_code = process_daily_data()
    
    # Podsumowanie
    elapsed_time = time.time() - start_time
    logging.info(f"Zakończenie procesu ETL: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Całkowity czas wykonania: {elapsed_time:.2f} sekund")
    
    return return_code

if __name__ == "__main__":
    sys.exit(main())