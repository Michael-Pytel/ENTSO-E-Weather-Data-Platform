# config.py - Konfiguracja dla wszystkich skryptów
CONFIG = {
    # Konfiguracja ENTSO-E
    'entsoe_api_key': '14911a12-10be-4bcb-a74a-64195e416851',
    'country_code': 'PL',  # Kod kraju (Polska)
    
    # Konfiguracja Open-Meteo
    'locations': [
        {'name': 'Warszawa', 'latitude': 52.23, 'longitude': 21.01},
        {'name': 'Kraków', 'latitude': 50.06, 'longitude': 19.94},
        {'name': 'Gdańsk', 'latitude': 54.35, 'longitude': 18.65}
    ],
    
    # Konfiguracja SQL Server
    'sql_server': 'localhost',
    'sql_database': 'EntsoeWeatherDW',
    'sql_username': 'sa',
    'sql_password': 'dupa123',
    
    # Ścieżki do plików tymczasowych
    'temp_folder': 'C:/Temp/ENTSO_ETL'
}