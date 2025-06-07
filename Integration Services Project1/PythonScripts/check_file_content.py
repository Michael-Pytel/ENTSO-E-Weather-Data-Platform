"""
check_file_content.py
Sprawdza zawartość pliku EurostatClient.py
"""

import os
import sys

def check_file_content():
    """Sprawdza zawartość pliku EurostatClient.py"""
    
    print("="*60)
    print("SPRAWDZANIE ZAWARTOŚCI PLIKU")
    print("="*60)
    
    # Sprawdź czy plik istnieje
    client_file = "EurostatClient.py"
    
    if not os.path.exists(client_file):
        print(f"❌ Plik {client_file} nie istnieje!")
        return
    
    print(f"✅ Plik {client_file} istnieje")
    
    # Odczytaj zawartość
    try:
        with open(client_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"📄 Rozmiar pliku: {len(content)} znaków")
        
        # Sprawdź kluczowe fragmenty
        print(f"\n🔍 Sprawdzanie kluczowych fragmentów:")
        
        # 1. Sprawdź PKB
        if "'unit': 'CP_EUR_HAB'" in content:
            print("✅ PKB: Poprawna jednostka CP_EUR_HAB")
        elif "'unit': 'EUR_HAB'" in content:
            print("❌ PKB: Stara jednostka EUR_HAB - WYMAGA POPRAWY!")
        else:
            print("⚠️  PKB: Nie znaleziono jednostki")
        
        # 2. Sprawdź intensywność energetyczną
        if "'unit': 'EUR_KGOE'" in content:
            print("✅ Intensywność: Poprawna jednostka EUR_KGOE")
        elif "'unit': 'KGOE_TEUR'" in content:
            print("❌ Intensywność: Stara jednostka KGOE_TEUR - WYMAGA POPRAWY!")
        else:
            print("⚠️  Intensywność: Nie znaleziono jednostki")
        
        # 3. Sprawdź bezrobocie
        if "'age': 'TOTAL'" in content:
            print("❌ Bezrobocie: Ma parametr age=TOTAL - WYMAGA USUNIĘCIA!")
        else:
            print("✅ Bezrobocie: Brak problematycznego parametru age")
        
        # 4. Sprawdź datę modyfikacji
        mod_time = os.path.getmtime(client_file)
        from datetime import datetime
        mod_date = datetime.fromtimestamp(mod_time)
        print(f"\n📅 Data modyfikacji: {mod_date}")
        
        # 5. Pokaż fragment z datasets
        print(f"\n📋 Fragment z konfiguracją datasets:")
        lines = content.split('\n')
        
        in_datasets = False
        datasets_lines = []
        
        for i, line in enumerate(lines):
            if 'self.datasets = {' in line:
                in_datasets = True
                datasets_lines.append(f"{i+1:4}: {line}")
            elif in_datasets:
                datasets_lines.append(f"{i+1:4}: {line}")
                if line.strip() == '}' and len([l for l in datasets_lines if '{' in l]) == len([l for l in datasets_lines if '}' in l]):
                    break
        
        if datasets_lines:
            print("Pierwsze 20 linii konfiguracji:")
            for line in datasets_lines[:20]:
                if 'unit' in line or 'age' in line:
                    print(f"➡️  {line}")
                else:
                    print(f"   {line}")
        else:
            print("❌ Nie znaleziono konfiguracji datasets!")
        
    except Exception as e:
        print(f"❌ Błąd czytania pliku: {str(e)}")

def suggest_manual_fix():
    """Zasugeruj ręczną poprawę"""
    
    print(f"\n" + "="*60)
    print("INSTRUKCJA RĘCZNEJ POPRAWY")
    print("="*60)
    
    print(f"\n📝 Jeśli plik ma stare parametry, otwórz EurostatClient.py i znajdź:")
    print(f"   self.datasets = {{")
    print(f"\n🔧 Zamień następujące linie:")
    
    print(f"\n1. W gdp_per_capita:")
    print(f"   STARE: 'unit': 'EUR_HAB'")
    print(f"   NOWE:  'unit': 'CP_EUR_HAB'")
    
    print(f"\n2. W energy_intensity:")
    print(f"   STARE: 'unit': 'KGOE_TEUR'")
    print(f"   NOWE:  'unit': 'EUR_KGOE'")
    
    print(f"\n3. W unemployment_rate:")
    print(f"   USUŃ:  'age': 'TOTAL',")
    print(f"   (zostaw tylko 'sex': 'T' i 'unit': 'PC_ACT')")
    
    print(f"\n💾 Zapisz plik i uruchom ponownie test")

def create_fixed_snippet():
    """Utwórz fragment kodu do skopiowania"""
    
    print(f"\n" + "="*60)
    print("GOTOWY FRAGMENT DO SKOPIOWANIA")
    print("="*60)
    
    fixed_config = '''
        # Konfiguracja datasetów Eurostat - NAPRAWIONE
        self.datasets = {
            'population': {
                'code': 'demo_pjan',
                'description': 'Population by sex and age',
                'params': {
                    'sex': 'T',      # Total
                    'age': 'TOTAL'   # All ages
                }
            },
            'gdp_per_capita': {
                'code': 'nama_10_pc',
                'description': 'GDP per capita',
                'params': {
                    'unit': 'CP_EUR_HAB',  # POPRAWKA!
                    'na_item': 'B1GQ'      # Gross domestic product at market prices
                },
                'alternative_units': ['CP_PPS_EU27_2020_HAB', 'CLV10_EUR_HAB']
            },
            'electricity_prices': {
                'code': 'nrg_pc_204',
                'description': 'Electricity prices for household consumers',
                'params': {
                    'unit': 'KWH',         # Per kWh
                    'product': '6000',     # Electricity
                    'nrg_cons': 'KWH2500-4999',  # Band DC
                    'tax': 'X_TAX',        # Excluding taxes
                    'currency': 'EUR'
                }
            },
            'energy_intensity': {
                'code': 'sdg_07_30',
                'description': 'Energy intensity of the economy',
                'params': {
                    'unit': 'EUR_KGOE'  # POPRAWKA!
                },
                'alternative_units': ['PPS_KGOE', 'KGOE_TEUR']
            },
            'unemployment_rate': {
                'code': 'une_rt_a',
                'description': 'Unemployment rate by sex and age',
                'params': {
                    'sex': 'T',      # Total
                    'unit': 'PC_ACT' # Percentage of active population
                    # POPRAWKA: usunięto 'age': 'TOTAL'
                },
                'alternative_units': ['THS_PER', 'PC_POP'],
                'alternative_age_params': ['Y15-74', 'Y15-24', 'Y25-54']
            }
        }
    '''
    
    print("📋 Skopiuj poniższy fragment i zastąp nim konfigurację datasets:")
    print(fixed_config)

def main():
    """Główna funkcja sprawdzająca"""
    
    print("SPRAWDZANIE PLIKU EUROSTAT CLIENT")
    print("="*60)
    
    try:
        check_file_content()
        suggest_manual_fix()
        create_fixed_snippet()
        
        print(f"\n✅ Sprawdzanie zakończone")
        print(f"📝 Po wprowadzeniu poprawek uruchom: python simple_diagnostic_test.py")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ BŁĄD: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)