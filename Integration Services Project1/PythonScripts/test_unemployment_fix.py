"""
test_unemployment_fix.py
Specjalny test dla naprawy datasetu bezrobocia
"""

import requests
import json
import logging

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('unemployment_test')

def test_unemployment_parameters():
    """Test różnych kombinacji parametrów dla bezrobocia"""
    
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_a"
    
    # Test przypadki
    test_cases = [
        {
            'name': 'Bez age parameter',
            'params': {
                'sex': 'T',
                'unit': 'PC_ACT',
                'geo': 'DE',
                'time': '2023',
                'format': 'JSON',
                'lang': 'EN'
            }
        },
        {
            'name': 'Age Y15-74',
            'params': {
                'sex': 'T',
                'age': 'Y15-74',
                'unit': 'PC_ACT',
                'geo': 'DE',
                'time': '2023',
                'format': 'JSON',
                'lang': 'EN'
            }
        },
        {
            'name': 'Age Y15-64',
            'params': {
                'sex': 'T',
                'age': 'Y15-64',
                'unit': 'PC_ACT',
                'geo': 'DE',
                'time': '2023',
                'format': 'JSON',
                'lang': 'EN'
            }
        },
        {
            'name': 'Age Y_GE15',
            'params': {
                'sex': 'T',
                'age': 'Y_GE15',
                'unit': 'PC_ACT',
                'geo': 'DE',
                'time': '2023',
                'format': 'JSON',
                'lang': 'EN'
            }
        },
        {
            'name': 'Unit THS_PER',
            'params': {
                'sex': 'T',
                'age': 'Y15-74',
                'unit': 'THS_PER',
                'geo': 'DE',
                'time': '2023',
                'format': 'JSON',
                'lang': 'EN'
            }
        },
        {
            'name': 'Minimalne parametry - tylko geo i time',
            'params': {
                'geo': 'DE',
                'time': '2023',
                'format': 'JSON',
                'lang': 'EN'
            }
        }
    ]
    
    print("="*70)
    print("TEST PARAMETRÓW BEZROBOCIA (une_rt_a)")
    print("="*70)
    
    working_configs = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['name']}")
        print(f"   Parametry: {test_case['params']}")
        
        try:
            response = requests.get(base_url, params=test_case['params'], timeout=30)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                values_count = len(data.get('value', {}))
                print(f"   Liczba wartości: {values_count}")
                
                if values_count > 0:
                    print("   ✅ SUKCES - Zwraca dane!")
                    working_configs.append(test_case)
                    
                    # Zapisz udaną konfigurację
                    filename = f"unemployment_success_{i}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"   Zapisano do: {filename}")
                    
                    # Pokaż przykładowe wartości
                    sample_values = list(data['value'].items())[:3]
                    print(f"   Przykład: {sample_values}")
                    
                    # Pokaż dostępne wymiary
                    if 'dimension' in data:
                        if 'age' in data['dimension']:
                            age_options = list(data['dimension']['age']['category']['index'].keys())
                            print(f"   Dostępne age: {age_options[:5]}")
                        
                        if 'unit' in data['dimension']:
                            unit_options = list(data['dimension']['unit']['category']['index'].keys())
                            print(f"   Dostępne unit: {unit_options}")
                else:
                    print("   ⚠️  Brak wartości w odpowiedzi")
                    
                    # Sprawdź pozycje bez danych
                    if 'extension' in data and 'positions-with-no-data' in data['extension']:
                        no_data = data['extension']['positions-with-no-data']
                        print(f"   Pozycje bez danych: {no_data}")
            else:
                print(f"   ❌ Błąd HTTP: {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"   Szczegóły: {error_data}")
                except:
                    print(f"   Treść: {response.text[:200]}")
                    
        except Exception as e:
            print(f"   ❌ Wyjątek: {str(e)}")
    
    print("\n" + "="*70)
    print("PODSUMOWANIE")
    print("="*70)
    
    if working_configs:
        print(f"\nZnaleziono {len(working_configs)} działających konfiguracji:")
        for config in working_configs:
            print(f"  ✅ {config['name']}")
        
        print(f"\nNajlepsza konfiguracja: {working_configs[0]['name']}")
        print(f"Parametry: {working_configs[0]['params']}")
        
        return working_configs[0]['params']
    else:
        print("\n❌ Żadna konfiguracja nie działa!")
        return None

if __name__ == "__main__":
    working_params = test_unemployment_parameters()
    
    if working_params:
        print(f"\n🎉 Użyj tych parametrów w EurostatClient:")
        print(f"params = {working_params}")
    else:
        print(f"\n😞 Trzeba dalej szukać działających parametrów")