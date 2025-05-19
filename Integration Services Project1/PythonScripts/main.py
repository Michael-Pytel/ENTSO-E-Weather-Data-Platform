# main.py - Główny skrypt ETL
import sys
import os
import subprocess
from datetime import datetime, timedelta
import time

def run_script(script_name, params=None):
    """Uruchamia skrypt Python z podanymi parametrami"""
    cmd = [sys.executable, script_name]
    if params:
        cmd.extend(params)
    
    print(f"Uruchamianie: {' '.join(cmd)}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    print("STDOUT:")
    print(stdout)
    
    if stderr:
        print("STDERR:")
        print(stderr)
    
    return process.returncode

def main():
    """Główna funkcja ETL"""
    start_time = time.time()
    print(f"Rozpoczęcie procesu ETL: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(scripts_dir)
    
    # Ustalenie dat dla ETL
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Etap 1: Pobieranie danych ENTSO-E
    print("\n=== Etap 1: Pobieranie danych ENTSO-E ===")
    
    # Pobieranie danych o obciążeniu
    return_code = run_script('entsoe_loader.py', ['load', start_date, end_date])
    if return_code != 0:
        print("Błąd podczas pobierania danych o obciążeniu")
        return return_code
    
    # Pobieranie danych o cenach
    return_code = run_script('entsoe_loader.py', ['price', start_date, end_date])
    if return_code != 0:
        print("Błąd podczas pobierania danych o cenach")
        return return_code
    
    # Pobieranie danych o generacji
    return_code = run_script('entsoe_loader.py', ['generation', start_date, end_date])
    if return_code != 0:
        print("Błąd podczas pobierania danych o generacji")
        return return_code
    
    # Etap 2: Pobieranie danych pogodowych
    print("\n=== Etap 2: Pobieranie danych pogodowych ===")
    return_code = run_script('weather_loader.py', [start_date, end_date])
    if return_code != 0:
        print("Błąd podczas pobierania danych pogodowych")
        return return_code
    
    # Etap 3: Integracja danych
    print("\n=== Etap 3: Integracja danych ===")
    return_code = run_script('data_integration.py')
    if return_code != 0:
        print("Błąd podczas integracji danych")
        return return_code
    
    # Podsumowanie
    elapsed_time = time.time() - start_time
    print(f"\nZakończenie procesu ETL: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Całkowity czas wykonania: {elapsed_time:.2f} sekund")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())