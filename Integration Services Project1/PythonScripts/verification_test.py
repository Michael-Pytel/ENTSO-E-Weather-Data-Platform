"""
test_final_client.py
Test ostatecznej wersji EurostatClient_FINAL.py
"""

import sys
import os

# Importuj nowy klient
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from EurostatClient import EurostatClient
    print("✅ Importowano EurostatClient_FINAL")
except ImportError as e:
    print(f"❌ Błąd importu EurostatClient_FINAL: {e}")
    try:
        from EurostatClient import EurostatClient
        print("⚠️  Używam starego EurostatClient")
    except ImportError as e2:
        print(f"❌ Nie można zaimportować żadnego klienta: {e2}")
        sys.exit(1)

def test_configuration():
    """Test konfiguracji w nowym kliencie"""
    
    print("\n" + "="*60)
    print("TEST KONFIGURACJI NOWEGO KLIENTA")
    print("="*60)
    
    client = EurostatClient()
    
    print("\n🔧 Sprawdzanie konfiguracji datasets:")
    
    # Sprawdź PKB
    gdp_config = client.datasets['gdp_per_capita']['params']
    gdp_unit = gdp_config.get('unit', 'BRAK')
    print(f"PKB unit: {gdp_unit}")
    
    if gdp_unit == 'CP_EUR_HAB':
        print("✅ PKB: Poprawna jednostka")
    else:
        print(f"❌ PKB: Błędna jednostka, powinno być CP_EUR_HAB")
    
    # Sprawdź intensywność
    energy_config = client.datasets['energy_intensity']['params']
    energy_unit = energy_config.get('unit', 'BRAK')
    print(f"Intensywność unit: {energy_unit}")
    
    if energy_unit == 'EUR_KGOE':
        print("✅ Intensywność: Poprawna jednostka")
    else:
        print(f"❌ Intensywność: Błędna jednostka, powinno być EUR_KGOE")
    
    # Sprawdź bezrobocie
    unemp_config = client.datasets['unemployment_rate']['params']
    print(f"Bezrobocie params: {unemp_config}")
    
    if 'age' not in unemp_config:
        print("✅ Bezrobocie: Brak problematycznego parametru age")
    else:
        print(f"❌ Bezrobocie: Ma parametr age={unemp_config['age']}")
    
    # Ogólna ocena konfiguracji
    config_score = 0
    if gdp_unit == 'CP_EUR_HAB': config_score += 1
    if energy_unit == 'EUR_KGOE': config_score += 1
    if 'age' not in unemp_config: config_score += 1
    
    print(f"\n📊 Wynik konfiguracji: {config_score}/3")
    
    return config_score == 3

def test_individual_methods():
    """Test poszczególnych metod"""
    
    print("\n" + "="*60)
    print("TEST POSZCZEGÓLNYCH METOD")
    print("="*60)
    
    client = EurostatClient()
    countries = ['DE', 'FR', 'PL']
    
    methods_to_test = [
        ('population', client.get_population_data),
        ('gdp_per_capita', client.get_gdp_per_capita_data),
        ('electricity_prices', client.get_electricity_prices_data),
        ('energy_intensity', client.get_energy_intensity_data),
        ('unemployment_rate', client.get_unemployment_data)
    ]
    
    results = {}
    
    for dataset_name, method in methods_to_test:
        print(f"\n🧪 Test {dataset_name}:")
        
        try:
            df = method(countries, 2023)
            
            total_records = len(df)
            valid_records = len(df[df['value'].notna()]) if not df.empty else 0
            coverage = (valid_records / total_records * 100) if total_records > 0 else 0
            
            print(f"   Rekordy: {valid_records}/{total_records} ({coverage:.1f}%)")
            
            if valid_records > 0:
                print("   ✅ SUKCES!")
                sample = df[df['value'].notna()].iloc[0]
                print(f"   Przykład: {sample['country_name']} ({sample['time_period']}): {sample['value']}")
                results[dataset_name] = 'SUCCESS'
            else:
                print("   ❌ BRAK DANYCH")
                results[dataset_name] = 'NO_DATA'
                
        except Exception as e:
            print(f"   ❌ BŁĄD: {str(e)}")
            results[dataset_name] = 'ERROR'
    
    success_count = len([r for r in results.values() if r == 'SUCCESS'])
    print(f"\n📊 Wynik metod: {success_count}/5")
    
    return results, success_count

def test_full_pipeline():
    """Test pełnego pipeline'u"""
    
    print("\n" + "="*60)
    print("TEST PEŁNEGO PIPELINE'U")
    print("="*60)
    
    client = EurostatClient()
    countries = ['DE', 'FR', 'PL']
    
    try:
        print("\n📊 Uruchamianie pełnej ekstrakcji...")
        
        # Pobierz wszystkie dane
        raw_data = client.extract_all_socioeconomic_data(2020, countries)
        
        # Przygotuj zintegrowane dane
        integrated_data = client.prepare_integrated_socioeconomic_data(raw_data)
        
        print(f"   Surowe datasety: {len(raw_data)}")
        print(f"   Zintegrowane profile: {len(integrated_data)}")
        
        # Sprawdź pokrycie danych
        good_datasets = 0
        total_datasets = len(raw_data)
        
        print(f"\n📋 Pokrycie danych:")
        for dataset_name, df in raw_data.items():
            valid_records = len(df[df['value'].notna()]) if not df.empty else 0
            total_records = len(df) if not df.empty else 0
            coverage = (valid_records / total_records * 100) if total_records > 0 else 0
            
            if coverage >= 50:
                good_datasets += 1
                icon = "✅"
            elif coverage > 0:
                icon = "⚠️"
            else:
                icon = "❌"
            
            print(f"   {icon} {dataset_name}: {valid_records}/{total_records} ({coverage:.1f}%)")
        
        # Sprawdź jakość zintegrowanych danych
        if not integrated_data.empty:
            avg_quality = integrated_data['data_quality_score'].mean()
            max_quality = integrated_data['data_quality_score'].max()
            
            print(f"\n🎯 Jakość zintegrowanych danych:")
            print(f"   Średnia jakość: {avg_quality:.1f}/5.0")
            print(f"   Maksymalna jakość: {max_quality}/5.0")
        
        success_rate = (good_datasets / total_datasets * 100) if total_datasets > 0 else 0
        print(f"\n📊 Sukces pipeline'u: {success_rate:.1f}% ({good_datasets}/{total_datasets})")
        
        return success_rate >= 80, success_rate
        
    except Exception as e:
        print(f"   ❌ BŁĄD PIPELINE'U: {str(e)}")
        return False, 0

def generate_final_report(config_ok, method_results, method_success, pipeline_ok, pipeline_rate):
    """Generuje końcowy raport"""
    
    print("\n" + "="*60)
    print("KOŃCOWY RAPORT - NOWY KLIENT")
    print("="*60)
    
    print(f"\n1. ⚙️  Konfiguracja: {'✅ POPRAWNA' if config_ok else '❌ BŁĘDNA'}")
    print(f"2. 🧪 Metody indywidualne: {method_success}/5 ({'✅' if method_success >= 4 else '❌'})")
    print(f"3. 🔄 Pipeline: {'✅ DOBRY' if pipeline_ok else '❌ SŁABY'} ({pipeline_rate:.1f}%)")
    
    print(f"\n📋 Szczegóły metod:")
    for dataset, result in method_results.items():
        icon = "✅" if result == 'SUCCESS' else "❌"
        print(f"   {icon} {dataset}: {result}")
    
    # Ocena końcowa
    if config_ok and method_success >= 4 and pipeline_ok:
        grade = "DOSKONAŁY"
        print("\n🎉 DOSKONAŁY WYNIK! Nowy klient w pełni funkcjonalny!")
        next_steps = "✅ Gotowy do użycia produkcyjnego!"
    elif config_ok and method_success >= 3:
        grade = "DOBRY"
        print("\n👍 DOBRY WYNIK! Większość funkcji działa poprawnie!")
        next_steps = "⚠️  Można używać, ale monitoruj niedziałające datasety"
    elif config_ok:
        grade = "ŚREDNI"
        print("\n⚠️  ŚREDNI WYNIK. Konfiguracja dobra, ale problemy z API.")
        next_steps = "🔧 Sprawdź połączenie z API i parametry"
    else:
        grade = "SŁABY"
        print("\n❌ SŁABY WYNIK. Problemy z konfiguracją.")
        next_steps = "📝 Sprawdź plik EurostatClient_FINAL.py"
    
    print(f"\n🔧 Następne kroki: {next_steps}")
    
    if grade in ["DOSKONAŁY", "DOBRY"]:
        print(f"\n📝 Instrukcje:")
        print(f"   1. Skopiuj EurostatClient_FINAL.py jako EurostatClient.py")
        print(f"   2. Uruchom: python EurostatClient.py")
        print(f"   3. Sprawdź pliki CSV w katalogu eurostat_export/")
    
    return grade

def main():
    """Główna funkcja testowa"""
    
    print("TEST OSTATECZNEJ WERSJI EUROSTAT CLIENT")
    print("="*60)
    
    try:
        # Test 1: Konfiguracja
        config_ok = test_configuration()
        
        # Test 2: Metody indywidualne
        method_results, method_success = test_individual_methods()
        
        # Test 3: Pełny pipeline
        pipeline_ok, pipeline_rate = test_full_pipeline()
        
        # Raport końcowy
        grade = generate_final_report(config_ok, method_results, method_success, pipeline_ok, pipeline_rate)
        
        # Kod wyjścia
        if grade == "DOSKONAŁY":
            return 0
        elif grade == "DOBRY":
            return 0
        elif grade == "ŚREDNI":
            return 1
        else:
            return 2
        
    except Exception as e:
        print(f"\n❌ BŁĄD KRYTYCZNY: {str(e)}")
        import traceback
        traceback.print_exc()
        return 3

if __name__ == "__main__":
    exit_code = main()
    print(f"\nKod wyjścia: {exit_code}")
    sys.exit(exit_code)