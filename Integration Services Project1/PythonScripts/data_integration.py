# entso_weather_integration.py - Integracja danych energetycznych i pogodowych
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
from utils import setup_logging, connect_to_sql, load_dataframe_to_sql
from config import CONFIG

def integrate_energy_weather_data(start_date=None, end_date=None):
    """Integracja danych energetycznych z pogodowymi"""
    setup_logging('integration')
    
    # Ustalenie zakresu dat, jeśli nie podano
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    logging.info(f"Rozpoczęcie integracji danych dla okresu {start_date} do {end_date}")
    
    try:
        conn = connect_to_sql()
        
        # 1. Przygotowanie map wymiaru daty i czasu
        logging.info("Przygotowanie wymiarów daty i czasu")
        with conn:
            cursor = conn.cursor()
            
            # Aktualizacja wymiaru daty
            cursor.execute("""
                MERGE INTO dim_date AS target
                USING (
                    SELECT DISTINCT 
                        CAST(FORMAT(DateTime, 'yyyyMMdd') AS INT) AS date_id,
                        CAST(DateTime AS DATE) AS full_date,
                        DATENAME(WEEKDAY, DateTime) AS day_of_week,
                        DAY(DateTime) AS day_of_month,
                        MONTH(DateTime) AS month,
                        DATENAME(MONTH, DateTime) AS month_name,
                        DATEPART(QUARTER, DateTime) AS quarter,
                        YEAR(DateTime) AS year,
                        CASE 
                            WHEN MONTH(DateTime) IN (12, 1, 2) THEN 'Winter'
                            WHEN MONTH(DateTime) IN (3, 4, 5) THEN 'Spring'
                            WHEN MONTH(DateTime) IN (6, 7, 8) THEN 'Summer'
                            ELSE 'Fall'
                        END AS season,
                        0 AS is_holiday, -- Placeholder, można zaktualizować później
                        NULL AS holiday_name,
                        NULL AS holiday_type,
                        CASE 
                            WHEN DATENAME(WEEKDAY, DateTime) IN ('Saturday', 'Sunday') THEN 0 ELSE 1
                        END AS is_school_day,
                        CASE 
                            WHEN DATENAME(WEEKDAY, DateTime) IN ('Saturday', 'Sunday') THEN 1 ELSE 0
                        END AS is_weekend
                    FROM (
                        SELECT DateTime FROM StagingLoad
                        UNION
                        SELECT DateTime FROM StagingGeneration
                        UNION
                        SELECT DateTime FROM StagingPrice
                    ) AS combined_dates
                    WHERE DateTime BETWEEN ? AND ?
                ) AS source
                ON target.date_id = source.date_id
                WHEN NOT MATCHED THEN
                    INSERT (
                        date_id, full_date, day_of_week, day_of_month, month,
                        month_name, quarter, year, season, is_holiday, 
                        holiday_name, holiday_type, is_school_day, is_weekend
                    )
                    VALUES (
                        source.date_id, source.full_date, source.day_of_week, source.day_of_month, source.month,
                        source.month_name, source.quarter, source.year, source.season, source.is_holiday,
                        source.holiday_name, source.holiday_type, source.is_school_day, source.is_weekend
                    );
            """, start_date, end_date)
            
            # Aktualizacja wymiaru czasu
            cursor.execute("""
                MERGE INTO dim_time AS target
                USING (
                    SELECT DISTINCT 
                        DATEPART(HOUR, DateTime) * 60 + DATEPART(MINUTE, DateTime) AS time_id,
                        DATEPART(HOUR, DateTime) AS hour,
                        DATEPART(MINUTE, DateTime) AS minute,
                        CASE 
                            WHEN DATEPART(HOUR, DateTime) < 6 THEN 'Night'
                            WHEN DATEPART(HOUR, DateTime) < 12 THEN 'Morning'
                            WHEN DATEPART(HOUR, DateTime) < 18 THEN 'Afternoon'
                            ELSE 'Evening'
                        END AS day_period,
                        CASE 
                            WHEN DATEPART(HOUR, DateTime) BETWEEN 7 AND 9 OR 
                                 DATEPART(HOUR, DateTime) BETWEEN 16 AND 18 THEN 1
                            ELSE 0
                        END AS is_peak_hour
                    FROM (
                        SELECT DateTime FROM StagingLoad
                        UNION
                        SELECT DateTime FROM StagingGeneration
                        UNION
                        SELECT DateTime FROM StagingPrice
                    ) AS combined_times
                    WHERE DateTime BETWEEN ? AND ?
                ) AS source
                ON target.time_id = source.time_id
                WHEN NOT MATCHED THEN
                    INSERT (time_id, hour, minute, day_period, is_peak_hour)
                    VALUES (source.time_id, source.hour, source.minute, source.day_period, source.is_peak_hour);
            """, start_date, end_date)
            
            # 2. Aktualizacja wymiarów strefy ofertowej i typów generacji
            logging.info("Aktualizacja wymiarów strefy ofertowej i typów generacji")
            
            # Aktualizacja dim_bidding_zone
            for zone in CONFIG['bidding_zones']:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dim_bidding_zone WHERE bidding_zone_code = ?)
                    BEGIN
                        INSERT INTO dim_bidding_zone (
                            bidding_zone_id, bidding_zone_code, bidding_zone_name, 
                            primary_country, timezone
                        )
                        VALUES (
                            NEXT VALUE FOR bidding_zone_seq, ?, ?, ?, 'Europe/Warsaw'
                        )
                    END
                """, zone['code'], zone['code'], zone['name'], zone['name'].split(' ')[0])
            
            # Aktualizacja dim_generation_type
            # Lista typów generacji z ENTSO-E
            generation_types = [
                ('Biomass', 'renewable', 'Biomass', 0, 'Biomass'),
                ('Fossil Brown coal/Lignite', 'conventional', 'Coal', 0, 'Lignite'),
                ('Fossil Coal-derived gas', 'conventional', 'Gas', 0, 'Coal gas'),
                ('Fossil Gas', 'conventional', 'Gas', 0, 'Natural gas'),
                ('Fossil Hard coal', 'conventional', 'Coal', 0, 'Hard coal'),
                ('Fossil Oil', 'conventional', 'Oil', 0, 'Oil'),
                ('Fossil Oil shale', 'conventional', 'Oil', 0, 'Oil shale'),
                ('Fossil Peat', 'conventional', 'Peat', 0, 'Peat'),
                ('Geothermal', 'renewable', 'Geothermal', 0, 'Geothermal'),
                ('Hydro Pumped Storage', 'conventional', 'Hydro', 0, 'Water'),
                ('Hydro Run-of-river and poundage', 'renewable', 'Hydro', 0, 'Water'),
                ('Hydro Water Reservoir', 'renewable', 'Hydro', 0, 'Water'),
                ('Marine', 'renewable', 'Marine', 1, 'Water'),
                ('Nuclear', 'conventional', 'Nuclear', 0, 'Nuclear'),
                ('Other', 'conventional', 'Other', 0, 'Various'),
                ('Other renewable', 'renewable', 'Other renewable', 1, 'Various'),
                ('Solar', 'renewable', 'Solar', 1, 'Solar'),
                ('Waste', 'conventional', 'Waste', 0, 'Waste'),
                ('Wind Offshore', 'renewable', 'Wind', 1, 'Wind'),
                ('Wind Onshore', 'renewable', 'Wind', 1, 'Wind')
            ]
            
            for i, (production_type, category, type_group, is_intermittent, fuel) in enumerate(generation_types, 1):
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dim_generation_type WHERE generation_type = ?)
                    BEGIN
                        INSERT INTO dim_generation_type (
                            generation_type_id, generation_category, generation_type, 
                            is_intermittent, fuel_source
                        )
                        VALUES (?, ?, ?, ?, ?)
                    END
                """, production_type, i, category, production_type, is_intermittent, fuel)
            
            # 3. Aktualizacja tabeli faktów
            logging.info("Aktualizacja tabeli faktów")
            
            # Integracja danych obciążenia i cen
            cursor.execute("""
                INSERT INTO fact_energy_weather (
                    date_id, time_id, bidding_zone_id, 
                    actual_consumption, forecasted_consumption, temperature_avg
                )
                SELECT 
                    CAST(FORMAT(l.DateTime, 'yyyyMMdd') AS INT) AS date_id,
                    DATEPART(HOUR, l.DateTime) * 60 + DATEPART(MINUTE, l.DateTime) AS time_id,
                    bz.bidding_zone_id,
                    l.ActualLoad AS actual_consumption,
                    l.ForecastedLoad AS forecasted_consumption,
                    w.temperature AS temperature_avg
                FROM 
                    StagingLoad l
                    JOIN dim_bidding_zone bz ON l.CountryCode = bz.bidding_zone_code
                    LEFT JOIN StagingWeather w ON 
                        CAST(l.DateTime AS DATE) = CAST(w.DateTime AS DATE) 
                        AND DATEPART(HOUR, l.DateTime) = DATEPART(HOUR, w.DateTime)
                        AND w.location IN (
                            SELECT TOP 1 name FROM CONFIG.locations 
                            WHERE country = l.CountryCode
                        )
                WHERE 
                    l.DateTime BETWEEN ? AND ?
                    AND NOT EXISTS (
                        SELECT 1 FROM fact_energy_weather f
                        JOIN dim_date d ON f.date_id = d.date_id
                        JOIN dim_time t ON f.time_id = t.time_id
                        WHERE 
                            d.full_date = CAST(l.DateTime AS DATE)
                            AND t.hour = DATEPART(HOUR, l.DateTime)
                            AND t.minute = DATEPART(MINUTE, l.DateTime)
                            AND f.bidding_zone_id = bz.bidding_zone_id
                    )
            """, start_date, end_date)
            
            # Aktualizacja danych generacji
            cursor.execute("""
                MERGE INTO fact_energy_weather AS target
                USING (
                    SELECT 
                        CAST(FORMAT(g.DateTime, 'yyyyMMdd') AS INT) AS date_id,
                        DATEPART(HOUR, g.DateTime) * 60 + DATEPART(MINUTE, g.DateTime) AS time_id,
                        bz.bidding_zone_id,
                        gt.generation_type_id,
                        g.Generation AS generation_amount,
                        NULL AS capacity_factor -- Placeholder
                    FROM 
                        StagingGeneration g
                        JOIN dim_bidding_zone bz ON g.CountryCode = bz.bidding_zone_code
                        JOIN dim_generation_type gt ON g.ProductionType = gt.generation_type
                    WHERE 
                        g.DateTime BETWEEN ? AND ?
                ) AS source
                ON (
                    target.date_id = source.date_id
                    AND target.time_id = source.time_id
                    AND target.bidding_zone_id = source.bidding_zone_id
                    AND (target.generation_type_id = source.generation_type_id OR (target.generation_type_id IS NULL AND source.generation_type_id IS NULL))
                )
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.generation_amount = source.generation_amount,
                        target.capacity_factor = source.capacity_factor
                WHEN NOT MATCHED THEN
                    INSERT (
                        date_id, time_id, bidding_zone_id, generation_type_id,
                        generation_amount, capacity_factor
                    )
                    VALUES (
                        source.date_id, source.time_id, source.bidding_zone_id, source.generation_type_id,
                        source.generation_amount, source.capacity_factor
                    );
            """, start_date, end_date)
            
            # Aktualizacja danych cenowych
            cursor.execute("""
                MERGE INTO fact_energy_weather AS target
                USING (
                    SELECT 
                        CAST(FORMAT(p.DateTime, 'yyyyMMdd') AS INT) AS date_id,
                        DATEPART(HOUR, p.DateTime) * 60 + DATEPART(MINUTE, p.DateTime) AS time_id,
                        bz.bidding_zone_id,
                        p.Price
                    FROM 
                        StagingPrice p
                        JOIN dim_bidding_zone bz ON p.CountryCode = bz.bidding_zone_code
                    WHERE 
                        p.DateTime BETWEEN ? AND ?
                ) AS source
                ON (
                    target.date_id = source.date_id
                    AND target.time_id = source.time_id
                    AND target.bidding_zone_id = source.bidding_zone_id
                )
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.electricity_price = source.Price
                WHEN NOT MATCHED THEN
                    INSERT (
                        date_id, time_id, bidding_zone_id, electricity_price
                    )
                    VALUES (
                        source.date_id, source.time_id, source.bidding_zone_id, source.Price
                    );
            """, start_date, end_date)
        
        logging.info("Zakończono integrację danych")
        return 0
        
    except Exception as e:
        logging.error(f"Błąd podczas integracji danych: {str(e)}")
        return 1

if __name__ == "__main__":
    # Parsowanie argumentów
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = None
        end_date = None
    
    sys.exit(integrate_energy_weather_data(start_date, end_date))