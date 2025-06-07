"""
final_validation_test.py
Ostateczny test walidacyjny naprawionego EurostatClient
"""

import sys
import os
import pandas as pd

# Dodaj ścieżkę do EurostatClient
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import naprawionego klienta
from EurostatClient import EurostatClient

def validate_data_quality(df, dataset_name):
    """Waliduje jakość danych w DataFrame"""
    
    if df.empty:
        return {
            'dataset': dataset_name,
            'status': 'EMPTY',
            'total_records': 0,
            'valid_records': 0,
            'coverage': 0
        }
    
    total_records = len(df)
    valid_records = len(df[df['value'].notna()])
    coverage = (valid_records / total_records * 100) if total_records > 0 else 0
    
    status = 'GOOD' if coverage >= 50 else 'POOR' if coverage > 0 else 'NO_DATA'
    
    return {
        'dataset': dataset_name,
        'status': status,
        'total_records': total_records,
        'valid_records': valid_records,
        'coverage': coverage
    }

def test_specific_parameters():
    """Test określonych parametrów które działają"""
    
    print("="*60)
    print("WALIDACJA KONKRETNYCH PARAMETRÓW")
    print("="*60)
    
    client = EurostatClient()
    
    # Test 1: PKB z poprawnymi parametrami
    print("\n1. TEST PKB z CP_EUR_HAB:")
    gdp_params = {
        'unit': 'CP_EUR_HAB',
        'na_item': 'B1GQ',
        'geo': ['DE', 'FR', 'PL'],
        'sinceTimePeriod': '2020'
    }
    
    try:
        response = client._make_request('nama_10_pc', gdp_params)
        if response and response.get('value'):
            print(f"   ✅ SUKCES - {len(response['value'])} wartości")
            # Pokazuj pierwsze 3 wartości
            sample_values = list(response['value'].items())[:3]
            print(f"   Przykłady: {sample_values}")
        else:
            print("   ❌ BRAK DANYCH")
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")
    
    # Test 2: Bezrobocie bez age parameter
    print("\n2. TEST BEZROBOCIA bez age:")
    unemp_params = {
        'sex': 'T',
        'unit': 'PC_ACT',
        'geo': ['DE', 'FR', 'PL'],
        'sinceTimePeriod': '2020'
    }
    
    try:
        response = client._make_request('une_rt_a', unemp_params)
        if response and response.get('value'):
            print(f"   ✅ SUKCES - {len(response['value'])} wartości")
            sample_values = list(response['value'].items())[:3]
            print(f"   Przykłady: {sample_values}")
        else:
            print("   ❌ BRAK DANYCH")
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")
    
    # Test 3: Intensywność energetyczna
    print("\n3. TEST INTENSYWNOŚCI z EUR_KGOE:")
    energy_params = {
        'unit': 'EUR_KGOE',
        'geo': ['DE', 'FR', 'PL'],
        'sinceTimePeriod': '2020'
    }
    
    try:
        response = client._make_request('sdg_07_30', energy_params)
        if response and response.get('value'):
            print(f"   ✅ SUKCES - {len(response['value'])} wartości")
            sample_values = list(response['value'].items())[:3]
            print(f"   Przykłady: {sample_values}")
        else:
            print("   ❌ BRAK DANYCH")
    except Exception as e:
        print(f"   ❌ BŁĄD: {str(e)}")

def test_full_pipeline():
    """Test pełnego pipeline'u klienta"""
    
    print("\n" + "="*60)
    print("TEST PEŁNEGO PIPELINE'U")
    print("="*60)
    
    client = EurostatClient()
    countries = ['DE', 'FR', 'PL']
    
    # Pobierz wszystkie dane
    print("\nPobieranie wszystkich danych...")
    raw_data = client.extract_all_socioeconomic_data(2020, countries)
    
    # Waliduj każdy dataset
    validation_results = []
    
    for dataset_name, df in raw_data.items():
        result = validate_data_quality(df, dataset_name)
        validation_results.append(result)
        
        status_icon = "✅" if result['status'] == 'GOOD' else "⚠️" if result['status'] == 'POOR' else "❌"
        print(f"\n{status_icon} {dataset_name.upper()}:")
        print(f"   Status: {result['status']}")
        print(f"   Rekordy: {result['valid_records']}/{result['total_records']}")
        print(f"   Pokrycie: {result['coverage']:.1f}%")
        
        # Pokaż przykład jeśli są dane
        if result['valid_records'] > 0:
            valid_records = df[df['value'].notna()]
            sample = valid_records.iloc[0]
            print(f"   Przykład: {sample['country_name']} ({sample['time_period']}): {sample['value']}")
    
    # Test zintegrowanych danych
    print(f"\nPrzygotowywanie zintegrowanych danych...")
    integrated_data = client.prepare_integrated_socioeconomic_data(raw_data)
    
    print(f"Liczba profili krajów: {len(integrated_data)}")
    
    if not integrated_data.empty:
        avg_quality = integrated_data['data_quality_score'].mean()
        max_quality = integrated_data['data_quality_score'].max()
        print(f"Średnia jakość danych: {avg_quality:.1f}/5.0")
        print(f"Maksymalna jakość: {max_quality}/5.0")
        
        # Pokaż najlepszy profil
        best_profile = integrated_data[integrated_data['data_quality_score'] == max_quality].iloc[0]
        print(f"\nNajlepszy profil: {best_profile['country_name']}")
        print(f"  Populacja: {best_profile.get('population', 'N/A')}")
        print(f"  PKB per capita: {best_profile.get('gdp_per_capita', 'N/A')}")
        print(f"  Cena energii: {best_profile.get('electricity_price_avg', 'N/A')}")
        print(f"  Intensywność energetyczna: {best_profile.get('energy_intensity', 'N/A')}")
        print(f"  Bezrobocie: {best_profile.get('unemployment_rate', 'N/A')}")
    
    return validation_results, integrated_data

def generate_final_report(validation_results, integrated_data):
    """Generuje końcowy raport"""
    
    print("\n" + "="*60)
    print("KOŃCOWY RAPORT")
    print("="*60)
    
    # Podsumowanie datasetów
    total_datasets = len(validation_results)
    good_datasets = len([r for r in validation_results if r['status'] == 'GOOD'])
    poor_datasets = len([r for r in validation_results if r['status'] == 'POOR'])
    failed_datasets = len([r for r in validation_results if r['status'] in ['NO_DATA', 'EMPTY']])
    
    print(f"\nPodsumowanie datasetów:")
    print(f"  ✅ Dobre (≥50% pokrycie): {good_datasets}")
    print(f"  ⚠️  Słabe (<50% pokrycie): {poor_datasets}")
    print(f"  ❌ Niedziałające: {failed_datasets}")
    print(f"  📊 Łącznie: {total_datasets}")
    
    # Wynik ogólny
    success_rate = (good_datasets + poor_datasets) / total_datasets * 100
    
    print(f"\nWynik ogólny: {success_rate:.1f}%")
    
    if success_rate >= 80:
        print("🎉 DOSKONAŁY WYNIK! EurostatClient gotowy do produkcji!")
        final_status = "EXCELLENT"
    elif success_rate >= 60:
        print("👍 DOBRY WYNIK! Większość funkcji działa.")
        final_status = "GOOD"
    elif success_rate >= 40:
        print("⚠️  ŚREDNI WYNIK. Wymaga poprawek.")
        final_status = "FAIR"
    else:
        print("❌ SŁABY WYNIK. Wymaga znacznych poprawek.")
        final_status = "POOR"
    
    # Szczegóły problematycznych datasetów
    problematic = [r for r in validation_results if r['status'] in ['NO_DATA', 'EMPTY', 'POOR']]
    if problematic:
        print(f"\nDatasets wymagające uwagi:")
        for result in problematic:
            print(f"  • {result['dataset']}: {result['status']} ({result['coverage']:.1f}% pokrycie)")
    
    # Rekomendacje
    print(f"\nRekomendacje:")
    if good_datasets >= 3:
        print("  ✅ System nadaje się do użycia produkcyjnego")
        print("  ✅ Większość kluczowych danych jest dostępna")
    
    if poor_datasets > 0:
        print("  ⚠️  Rozważ alternatywne źródła dla słabych datasetów")
    
    if failed_datasets > 0:
        print("  🔧 Wymagane dalsze debugowanie niedziałających datasetów")
    
    print("  💾 Pliki CSV zostały wyeksportowane do katalogu 'test_eurostat_export'")
    
    return final_status

def main():
    """Główna funkcja walidacyjna"""
    
    print("OSTATECZNA WALIDACJA EUROSTAT CLIENT")
    print("="*60)
    
    try:
        # Test konkretnych parametrów
        test_specific_parameters()
        
        # Test pełnego pipeline'u
        validation_results, integrated_data = test_full_pipeline()
        
        # Generuj końcowy raport
        final_status = generate_final_report(validation_results, integrated_data)
        
        # Kod wyjścia
        if final_status == "EXCELLENT":
            return 0
        elif final_status == "GOOD":
            return 0
        elif final_status == "FAIR":
            return 1
        else:
            return 2
    
    except Exception as e:
        print(f"\n❌ KRYTYCZNY BŁĄD: {str(e)}")
        import traceback
        traceback.print_exc()
        return 3

if __name__ == "__main__":
    exit_code = main()
    print(f"\nKod wyjścia: {exit_code}")
    sys.exit(exit_code)