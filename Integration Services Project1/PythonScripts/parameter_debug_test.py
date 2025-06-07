"""
parameter_debug_test.py
Debugowanie parametrów które są faktycznie wysyłane do API
"""

import sys
import os

# Dodaj ścieżkę do EurostatClient
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from EurostatClient import EurostatClient

def debug_gdp_parameters():
    """Debuguj parametry PKB"""
    
    print("="*60)
    print("DEBUGOWANIE PKB PER CAPITA")
    print("="*60)
    
    client = EurostatClient()
    
    # 1. Sprawdź co jest w konfiguracji
    gdp_config = client.datasets['gdp_per_capita']
    print(f"\n1. Konfiguracja w kliencie:")
    print(f"   Kod: {gdp_config['code']}")
    print(f"   Parametry: {gdp_config['params']}")
    
    # 2. Sprawdź co buduje metoda
    print(f"\n2. Co buduje metoda get_gdp_per_capita_data:")
    
    # Skopiuj dokładnie kod z metody
    dataset_info = client.datasets['gdp_per_capita']
    params = dataset_info['params'].copy()
    params['sinceTimePeriod'] = '2020'
    params['geo'] = ['DE', 'FR', 'PL']
    
    print(f"   Finalne parametry: {params}")
    
    # 3. Test bezpośredni z tymi parametrami
    print(f"\n3. Test bezpośredni:")
    
    try:
        response = client._make_request(dataset_info['code'], params)
        
        if response:
            values_count = len(response.get('value', {}))
            print(f"   Status: Odpowiedź otrzymana")
            print(f"   Wartości: {values_count}")
            
            if values_count > 0:
                print("   ✅ SUKCES!")
                sample_values = list(response['value'].items())[:3]
                print(f"   Przykład: {sample_values}")
            else:
                print("   ❌ PUSTE WARTOŚCI")
                
                # Sprawdź pozycje bez danych
                if 'extension' in response and 'positions-with-no-data' in response['extension']:
                    no_data = response['extension']['positions-with-no-data']
                    print(f"   Pozycje bez danych: {no_data}")
        else:
            print("   ❌ BRAK ODPOWIEDZI")
            
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")
    
    # 4. Test z parametrami które na pewno działają
    print(f"\n4. Test z parametrami które działają:")
    
    working_params = {
        'unit': 'CP_EUR_HAB',
        'na_item': 'B1GQ', 
        'geo': ['DE', 'FR', 'PL'],
        'sinceTimePeriod': '2020'
    }
    
    print(f"   Parametry które działają: {working_params}")
    
    try:
        response = client._make_request('nama_10_pc', working_params)
        
        if response:
            values_count = len(response.get('value', {}))
            print(f"   Wartości: {values_count}")
            
            if values_count > 0:
                print("   ✅ DZIAŁAJĄ!")
                
                # Porównaj różnice
                print(f"\n5. PORÓWNANIE PARAMETRÓW:")
                print(f"   Z konfiguracji: {params}")
                print(f"   Działające:     {working_params}")
                
                # Znajdź różnice
                differences = []
                for key in working_params:
                    if key not in params:
                        differences.append(f"Brakuje: {key}")
                    elif params[key] != working_params[key]:
                        differences.append(f"Różne {key}: '{params[key]}' vs '{working_params[key]}'")
                
                if differences:
                    print(f"   RÓŻNICE: {differences}")
                else:
                    print(f"   Parametry identyczne - dziwne!")
            else:
                print("   ❌ NADAL NIE DZIAŁAJĄ")
        else:
            print("   ❌ BRAK ODPOWIEDZI")
            
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")

def debug_unemployment_parameters():
    """Debuguj parametry bezrobocia"""
    
    print("\n" + "="*60)
    print("DEBUGOWANIE BEZROBOCIA")
    print("="*60)
    
    client = EurostatClient()
    
    # 1. Sprawdź konfigurację
    unemp_config = client.datasets['unemployment_rate']
    print(f"\n1. Konfiguracja w kliencie:")
    print(f"   Kod: {unemp_config['code']}")
    print(f"   Parametry: {unemp_config['params']}")
    
    # 2. Co buduje metoda
    print(f"\n2. Co buduje metoda get_unemployment_data:")
    
    dataset_info = client.datasets['unemployment_rate']
    params = dataset_info['params'].copy()
    params['sinceTimePeriod'] = '2020'
    params['geo'] = ['DE', 'FR', 'PL']
    
    print(f"   Finalne parametry: {params}")
    
    # 3. Test z tymi parametrami
    print(f"\n3. Test bezpośredni:")
    
    try:
        response = client._make_request(dataset_info['code'], params)
        
        if response:
            values_count = len(response.get('value', {}))
            print(f"   Wartości: {values_count}")
            
            if values_count > 0:
                print("   ✅ SUKCES!")
            else:
                print("   ❌ PUSTE WARTOŚCI")
        else:
            print("   ❌ BRAK ODPOWIEDZI")
            
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")
    
    # 4. Test z parametrami które działają
    print(f"\n4. Test z parametrami które działają:")
    
    working_params = {
        'sex': 'T',
        'unit': 'PC_ACT',
        'geo': ['DE', 'FR', 'PL'],
        'sinceTimePeriod': '2020'
    }
    
    print(f"   Parametry które działają: {working_params}")
    
    try:
        response = client._make_request('une_rt_a', working_params)
        
        if response:
            values_count = len(response.get('value', {}))
            print(f"   Wartości: {values_count}")
            
            if values_count > 0:
                print("   ✅ DZIAŁAJĄ!")
                
                # Porównaj różnice
                print(f"\n5. PORÓWNANIE PARAMETRÓW:")
                print(f"   Z konfiguracji: {params}")
                print(f"   Działające:     {working_params}")
                
                # Znajdź różnice
                differences = []
                for key in working_params:
                    if key not in params:
                        differences.append(f"Brakuje: {key}")
                    elif params[key] != working_params[key]:
                        differences.append(f"Różne {key}: '{params[key]}' vs '{working_params[key]}'")
                
                for key in params:
                    if key not in working_params:
                        differences.append(f"Nadmiarowy: {key}={params[key]}")
                
                if differences:
                    print(f"   RÓŻNICE: {differences}")
                else:
                    print(f"   Parametry identyczne")
            else:
                print("   ❌ NADAL NIE DZIAŁAJĄ")
        else:
            print("   ❌ BRAK ODPOWIEDZI")
            
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")

def debug_energy_intensity_parameters():
    """Debuguj parametry intensywności energetycznej"""
    
    print("\n" + "="*60)
    print("DEBUGOWANIE INTENSYWNOŚCI ENERGETYCZNEJ")
    print("="*60)
    
    client = EurostatClient()
    
    # 1. Sprawdź konfigurację
    energy_config = client.datasets['energy_intensity']
    print(f"\n1. Konfiguracja w kliencie:")
    print(f"   Kod: {energy_config['code']}")
    print(f"   Parametry: {energy_config['params']}")
    
    # 2. Co buduje metoda
    print(f"\n2. Co buduje metoda get_energy_intensity_data:")
    
    dataset_info = client.datasets['energy_intensity']
    params = dataset_info['params'].copy()
    params['sinceTimePeriod'] = '2020'
    params['geo'] = ['DE', 'FR', 'PL']
    
    print(f"   Finalne parametry: {params}")
    
    # 3. Test z parametrami które działają
    print(f"\n3. Test z parametrami które działają:")
    
    working_params = {
        'unit': 'EUR_KGOE',
        'geo': ['DE', 'FR', 'PL'],
        'sinceTimePeriod': '2020'
    }
    
    print(f"   Parametry które działają: {working_params}")
    
    try:
        response = client._make_request('sdg_07_30', working_params)
        
        if response:
            values_count = len(response.get('value', {}))
            print(f"   Wartości: {values_count}")
            
            if values_count > 0:
                print("   ✅ DZIAŁAJĄ!")
                
                # Porównaj różnice
                print(f"\n4. PORÓWNANIE PARAMETRÓW:")
                print(f"   Z konfiguracji: {params}")
                print(f"   Działające:     {working_params}")
                
                # Znajdź różnice
                if params != working_params:
                    print(f"   ❌ PARAMETRY SĄ RÓŻNE!")
                    for key in working_params:
                        if key not in params:
                            print(f"     Brakuje: {key}")
                        elif params[key] != working_params[key]:
                            print(f"     Różne {key}: '{params[key]}' vs '{working_params[key]}'")
                else:
                    print(f"   ✅ Parametry identyczne")
            else:
                print("   ❌ NADAL NIE DZIAŁAJĄ")
        else:
            print("   ❌ BRAK ODPOWIEDZI")
            
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")

def propose_fixes():
    """Zaproponuj poprawki"""
    
    print("\n" + "="*60)
    print("PROPONOWANE POPRAWKI")
    print("="*60)
    
    print("\nNa podstawie analizy, oto co trzeba naprawić:")
    
    print("\n1. 📝 Sprawdź plik EurostatClient.py - linia z 'datasets' config")
    print("   Upewnij się że jednostki to:")
    print("   - PKB: 'unit': 'CP_EUR_HAB' (nie EUR_HAB)")
    print("   - Intensywność: 'unit': 'EUR_KGOE' (nie KGOE_TEUR)")
    print("   - Bezrobocie: usuń 'age': 'TOTAL' z parametrów")
    
    print("\n2. 🔧 Jeśli config jest dobry, problem może być w:")
    print("   - Fallback mechanism nie działa")
    print("   - Parametry są nadpisywane gdzieś indziej")
    print("   - Problem z parsowaniem odpowiedzi")
    
    print("\n3. 🚀 Szybka naprawa:")
    print("   Uruchom: python parameter_debug_test.py")
    print("   Porównaj 'Z konfiguracji' vs 'Działające'")
    print("   Znajdź różnice i popraw je w datasets config")
    
    print("\n4. ✅ Test po naprawie:")
    print("   python simple_diagnostic_test.py")
    print("   Powinien pokazać sukces dla wszystkich datasetów")

def main():
    """Główna funkcja debugowania"""
    
    print("DEBUGOWANIE PARAMETRÓW EUROSTAT CLIENT")
    print("="*60)
    
    try:
        # Debug każdego datasetu
        debug_gdp_parameters()
        debug_unemployment_parameters() 
        debug_energy_intensity_parameters()
        
        # Zaproponuj poprawki
        propose_fixes()
        
        print("\n" + "="*60)
        print("KONIEC DEBUGOWANIA")
        print("="*60)
        print("\nUruchom ponownie po wprowadzeniu poprawek!")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ BŁĄD KRYTYCZNY: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)