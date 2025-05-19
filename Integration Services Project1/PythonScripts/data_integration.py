# data_integration_fixed.py - Integracja danych ENTSO-E i pogodowych - poprawiona wersja
import sys
import pandas as pd
import pyodbc
from utils import connect_to_sql, load_dataframe_to_sql
from config import CONFIG

def integrate_data():
    """Integracja danych z tablic przejściowych do tablic wymiarów i faktów"""
    conn = connect_to_sql()
    
    try:
        # Utworzenie tabeli wymiarów czasu, jeśli nie istnieje
        create_time_dimension(conn)
        
        # Utworzenie tabeli wymiarów lokalizacji, jeśli nie istnieje
        create_location_dimension(conn)
        
        # Utworzenie tabeli wymiarów kategorii pogodowych, jeśli nie istnieje
        create_weather_dimension(conn)
        
        # Integracja danych do tabeli faktów
        integrate_to_fact_table(conn)
        
        print("Integracja danych zakończona sukcesem")
        return 0
    except Exception as e:
        print(f"Błąd podczas integracji danych: {str(e)}")
        return 1
    finally:
        conn.close()

def create_time_dimension(conn):
    """Utworzenie i aktualizacja tabeli wymiarów czasu"""
    cursor = conn.cursor()
    
    # Utworzenie tabeli, jeśli nie istnieje
    cursor.execute("""
    IF OBJECT_ID('DimTime', 'U') IS NULL
    CREATE TABLE DimTime (
        TimeKey INT IDENTITY(1,1) PRIMARY KEY,
        DateTime DATETIME NOT NULL,
        Year INT NOT NULL,
        Quarter INT NOT NULL,
        Month INT NOT NULL,
        MonthName VARCHAR(10) NOT NULL,
        Day INT NOT NULL,
        DayOfWeek INT NOT NULL,
        DayName VARCHAR(10) NOT NULL,
        Hour INT NOT NULL,
        IsWeekend BIT NOT NULL,
        IsHoliday BIT NOT NULL
    )
    """)
    conn.commit()
    
    # Pobieranie dat z tabel przejściowych
    cursor.execute("""
    SELECT DISTINCT DateTime 
    FROM (
        SELECT DateTime FROM StagingLoad
        UNION
        SELECT DateTime FROM StagingPrice
        UNION
        SELECT DateTime FROM StagingWeather
    ) AS CombinedDates
    WHERE DateTime NOT IN (SELECT DateTime FROM DimTime)
    """)
    
    dates = cursor.fetchall()
    
    # Wstawianie nowych dat do wymiaru czasu
    if dates:
        print(f"Dodawanie {len(dates)} nowych dat do wymiaru czasu")
        
        for date_row in dates:
            dt = date_row[0]
            cursor.execute("""
            INSERT INTO DimTime (DateTime, Year, Quarter, Month, MonthName, Day, DayOfWeek, DayName, Hour, IsWeekend, IsHoliday)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
            dt, 
            dt.year, 
            (dt.month - 1) // 3 + 1,  # Kwartał
            dt.month,
            dt.strftime('%B'),  # Nazwa miesiąca
            dt.day,
            dt.weekday(),  # Dzień tygodnia (0=poniedziałek, 6=niedziela)
            dt.strftime('%A'),  # Nazwa dnia
            dt.hour,
            1 if dt.weekday() >= 5 else 0,  # Czy weekend
            0  # Placeholder dla świąt (do uzupełnienia osobno)
            )
        
        conn.commit()

def create_location_dimension(conn):
    """Utworzenie i aktualizacja tabeli wymiarów lokalizacji"""
    cursor = conn.cursor()
    
    # Utworzenie tabeli, jeśli nie istnieje
    cursor.execute("""
    IF OBJECT_ID('DimLocation', 'U') IS NULL
    CREATE TABLE DimLocation (
        LocationKey INT IDENTITY(1,1) PRIMARY KEY,
        LocationName NVARCHAR(50) NOT NULL,
        CountryCode NVARCHAR(10) NOT NULL,
        Latitude FLOAT NOT NULL,
        Longitude FLOAT NOT NULL
    )
    """)
    conn.commit()
    
    # Dodanie domyślnej lokalizacji kraju, jeśli nie istnieje
    cursor.execute("SELECT COUNT(*) FROM DimLocation WHERE CountryCode = ?", CONFIG['country_code'])
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
        INSERT INTO DimLocation (LocationName, CountryCode, Latitude, Longitude)
        VALUES (?, ?, ?, ?)
        """, 
        CONFIG['country_code'] + " (ogółem)", 
        CONFIG['country_code'],
        0,  # Placeholder dla współrzędnych kraju
        0
        )
        conn.commit()
    
    # Dodanie lokalizacji z konfiguracji, jeśli nie istnieją
    for location in CONFIG['locations']:
        cursor.execute("SELECT COUNT(*) FROM DimLocation WHERE LocationName = ?", location['name'])
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            INSERT INTO DimLocation (LocationName, CountryCode, Latitude, Longitude)
            VALUES (?, ?, ?, ?)
            """, 
            location['name'], 
            CONFIG['country_code'],
            location['latitude'],
            location['longitude']
            )
            conn.commit()

def create_weather_dimension(conn):
    """Utworzenie i aktualizacja tabeli wymiarów kategorii pogodowych"""
    cursor = conn.cursor()
    
    # Utworzenie tabeli, jeśli nie istnieje
    cursor.execute("""
    IF OBJECT_ID('DimWeather', 'U') IS NULL
    CREATE TABLE DimWeather (
        WeatherKey INT IDENTITY(1,1) PRIMARY KEY,
        TemperatureCategory NVARCHAR(20) NOT NULL,
        HumidityCategory NVARCHAR(20) NOT NULL,
        PrecipitationCategory NVARCHAR(20) NOT NULL,
        WindCategory NVARCHAR(20) NOT NULL,
        TemperatureMin FLOAT NOT NULL,
        TemperatureMax FLOAT NOT NULL,
        HumidityMin FLOAT NOT NULL,
        HumidityMax FLOAT NOT NULL,
        PrecipitationMin FLOAT NOT NULL,
        PrecipitationMax FLOAT NOT NULL,
        WindMin FLOAT NOT NULL,
        WindMax FLOAT NOT NULL
    )
    """)
    conn.commit()
    
    # Definiowanie kategorii pogodowych, jeśli nie istnieją
    categories = [
        # Temp, Humidity, Precip, Wind, TempMin, TempMax, HumMin, HumMax, PrecipMin, PrecipMax, WindMin, WindMax
        ("Bardzo zimno", "Sucho", "Brak", "Spokojnie", -50, 0, 0, 30, 0, 0.1, 0, 5),
        ("Zimno", "Sucho", "Brak", "Lekki wiatr", 0, 10, 0, 30, 0, 0.1, 5, 15),
        ("Zimno", "Wilgotno", "Słabe", "Lekki wiatr", 0, 10, 30, 70, 0.1, 2, 5, 15),
        ("Zimno", "Bardzo wilgotno", "Umiarkowane", "Umiarkowany wiatr", 0, 10, 70, 100, 2, 10, 15, 30),
        ("Umiarkowanie", "Sucho", "Brak", "Spokojnie", 10, 20, 0, 30, 0, 0.1, 0, 5),
        ("Umiarkowanie", "Wilgotno", "Słabe", "Lekki wiatr", 10, 20, 30, 70, 0.1, 2, 5, 15),
        ("Umiarkowanie", "Bardzo wilgotno", "Umiarkowane", "Umiarkowany wiatr", 10, 20, 70, 100, 2, 10, 15, 30),
        ("Ciepło", "Sucho", "Brak", "Spokojnie", 20, 30, 0, 30, 0, 0.1, 0, 5),
        ("Ciepło", "Wilgotno", "Słabe", "Lekki wiatr", 20, 30, 30, 70, 0.1, 2, 5, 15),
        ("Ciepło", "Bardzo wilgotno", "Umiarkowane", "Umiarkowany wiatr", 20, 30, 70, 100, 2, 10, 15, 30),
        ("Gorąco", "Sucho", "Brak", "Spokojnie", 30, 50, 0, 30, 0, 0.1, 0, 5),
        ("Gorąco", "Wilgotno", "Słabe", "Lekki wiatr", 30, 50, 30, 70, 0.1, 2, 5, 15),
        ("Gorąco", "Bardzo wilgotno", "Silne", "Silny wiatr", 30, 50, 70, 100, 10, 100, 30, 100)
    ]
    
    # Sprawdzenie, czy tabela jest pusta
    cursor.execute("SELECT COUNT(*) FROM DimWeather")
    if cursor.fetchone()[0] == 0:
        # Wstawianie kategorii
        for category in categories:
            cursor.execute("""
            INSERT INTO DimWeather (
                TemperatureCategory, HumidityCategory, PrecipitationCategory, WindCategory,
                TemperatureMin, TemperatureMax, HumidityMin, HumidityMax,
                PrecipitationMin, PrecipitationMax, WindMin, WindMax
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, category)
        
        conn.commit()

def integrate_to_fact_table(conn):
    """Integracja danych do tabeli faktów"""
    cursor = conn.cursor()
    
    # Utworzenie tabeli faktów, jeśli nie istnieje
    cursor.execute("""
    IF OBJECT_ID('FactEnergyWeather', 'U') IS NULL
    CREATE TABLE FactEnergyWeather (
        FactKey INT IDENTITY(1,1) PRIMARY KEY,
        TimeKey INT FOREIGN KEY REFERENCES DimTime(TimeKey),
        LocationKey INT FOREIGN KEY REFERENCES DimLocation(LocationKey),
        WeatherKey INT FOREIGN KEY REFERENCES DimWeather(WeatherKey),
        Load FLOAT NULL,
        Price FLOAT NULL,
        Temperature FLOAT NULL,
        Humidity FLOAT NULL,
        Precipitation FLOAT NULL,
        WindSpeed FLOAT NULL,
        CloudCover FLOAT NULL,
        Radiation FLOAT NULL
    )
    """)
    conn.commit()
    
    # Pobieranie danych z tablic przejściowych i łączenie
    cursor.execute("""
    -- Pomocnicze CTE do znalezienia klucza WeatherKey
    WITH WeatherCategories AS (
        SELECT 
            w.DateTime,
            w.Location,
            w.temperature,
            w.humidity,
            w.precipitation,
            w.wind_speed,
            w.cloud_cover,
            w.radiation,
            (SELECT TOP 1 WeatherKey FROM DimWeather 
             WHERE w.temperature BETWEEN TemperatureMin AND TemperatureMax
             AND w.humidity BETWEEN HumidityMin AND HumidityMax
             AND w.precipitation BETWEEN PrecipitationMin AND PrecipitationMax
             AND w.wind_speed BETWEEN WindMin AND WindMax) AS WeatherKey
        FROM StagingWeather w
    ),
    -- Agregacja danych obciążenia
    LoadData AS (
        SELECT DateTime, AVG([Actual Load]) AS LoadValue
        FROM StagingLoad
        GROUP BY DateTime
    ),
    -- Agregacja danych cen
    PriceData AS (
        SELECT DateTime, AVG(Price) AS PriceValue
        FROM StagingPrice
        GROUP BY DateTime
    )
    
    -- Wybieranie danych do integracji
    SELECT 
        t.TimeKey,
        loc.LocationKey,
        ISNULL(wc.WeatherKey, 1) AS WeatherKey,  -- 1 to domyślna wartość, jeśli nie ma dopasowania
        ld.LoadValue,
        pd.PriceValue,
        wc.temperature,
        wc.humidity,
        wc.precipitation,
        wc.wind_speed,
        wc.cloud_cover,
        wc.radiation
    FROM DimTime t
    JOIN DimLocation loc ON loc.CountryCode = ?
    LEFT JOIN LoadData ld ON t.DateTime = ld.DateTime
    LEFT JOIN PriceData pd ON t.DateTime = pd.DateTime
    LEFT JOIN WeatherCategories wc ON t.DateTime = wc.DateTime AND loc.LocationName = wc.Location
    WHERE t.DateTime >= ?
    AND t.DateTime <= ?
    AND NOT EXISTS (
        SELECT 1 
        FROM FactEnergyWeather f 
        WHERE f.TimeKey = t.TimeKey 
        AND f.LocationKey = loc.LocationKey
    )
    """, CONFIG['country_code'], 
    # ZMIANA: Używamy zakresu dat odpowiadającego danym w tabelach przejściowych
    pd.Timestamp('2020-01-01'),  # Data początkowa
    pd.Timestamp('2024-12-31'))  # Data końcowa
    
    rows = cursor.fetchall()
    
    # Wstawianie danych do tabeli faktów
    if rows:
        print(f"Dodawanie {len(rows)} nowych wierszy do tabeli faktów")
        
        # Dodajemy informację diagnostyczną
        print(f"Zakres dat: od {pd.Timestamp('2020-01-01')} do {pd.Timestamp('2024-12-31')}")
        
        # Aby uniknąć problemów z dużą ilością danych, przetwarzamy je partiami
        batch_size = 1000
        total_rows = len(rows)
        processed = 0
        
        while processed < total_rows:
            batch = rows[processed:processed + batch_size]
            for row in batch:
                cursor.execute("""
                INSERT INTO FactEnergyWeather (
                    TimeKey, LocationKey, WeatherKey, Load, Price,
                    Temperature, Humidity, Precipitation, WindSpeed, CloudCover, Radiation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
            
            conn.commit()
            processed += len(batch)
            print(f"Przetworzono {processed}/{total_rows} wierszy ({(processed/total_rows)*100:.1f}%)")
        
        print("Dane zintegrowane pomyślnie")
    else:
        print("Brak nowych danych do dodania")
        # Dodajemy diagnostykę
        print("Sprawdzamy czy istnieją dane w tabelach przejściowych:")
        cursor.execute("SELECT COUNT(*) FROM StagingLoad")
        load_count = cursor.fetchone()[0]
        print(f"StagingLoad: {load_count} wierszy")
        
        cursor.execute("SELECT COUNT(*) FROM StagingPrice")
        price_count = cursor.fetchone()[0]
        print(f"StagingPrice: {price_count} wierszy")
        
        cursor.execute("SELECT COUNT(*) FROM StagingWeather")
        weather_count = cursor.fetchone()[0]
        print(f"StagingWeather: {weather_count} wierszy")
        
        # Sprawdzamy czy tabele wymiarów mają dane
        cursor.execute("SELECT COUNT(*) FROM DimTime")
        time_count = cursor.fetchone()[0]
        print(f"DimTime: {time_count} wierszy")
        
        cursor.execute("SELECT COUNT(*) FROM DimLocation")
        loc_count = cursor.fetchone()[0]
        print(f"DimLocation: {loc_count} wierszy")
        
        cursor.execute("SELECT COUNT(*) FROM DimWeather")
        weather_dim_count = cursor.fetchone()[0]
        print(f"DimWeather: {weather_dim_count} wierszy")

if __name__ == "__main__":
    sys.exit(integrate_data())