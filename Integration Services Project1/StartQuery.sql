-- =======================================================
-- SKRYPTY DDL - HURTOWNIA DANYCH ENERGETYCZNO-POGODOWA
-- =======================================================

-- Utworzenie bazy danych
CREATE DATABASE EnergyWeatherDW;
GO

USE EnergyWeatherDW;
GO

-- =======================================================
-- TABELE WYMIAROWE
-- =======================================================

-- Wymiar daty
CREATE TABLE dim_date (
    date_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    full_date DATE NOT NULL DEFAULT '1900-01-01',
    day_of_week NVARCHAR(10) NOT NULL DEFAULT 'Unknown',
    day_of_month TINYINT NOT NULL DEFAULT 0,
    month TINYINT NOT NULL DEFAULT 0,
    month_name NVARCHAR(10) NOT NULL DEFAULT 'Unknown',
    quarter TINYINT NOT NULL DEFAULT 0,
    year SMALLINT NOT NULL DEFAULT 0,
    season NVARCHAR(10) NOT NULL DEFAULT 'Unknown',
    is_holiday NVARCHAR(3) NOT NULL DEFAULT 'No',
    holiday_name NVARCHAR(50) DEFAULT '',
    holiday_type NVARCHAR(30) DEFAULT '',
    is_school_day NVARCHAR(3) NOT NULL DEFAULT 'Yes',
    is_weekend NVARCHAR(3) NOT NULL DEFAULT 'No'
);

-- Wymiar czasu
CREATE TABLE dim_time (
    time_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    hour INT NOT NULL DEFAULT 0,
    minute INT NOT NULL DEFAULT 0,
    day_period NVARCHAR(10) NOT NULL DEFAULT 'Unknown',
    is_peak_hour NVARCHAR(3) NOT NULL DEFAULT 'No'
);

-- Wymiar stref przetargowych
CREATE TABLE dim_bidding_zone (
    bidding_zone_id TINYINT IDENTITY(1,1) PRIMARY KEY,
    bidding_zone_code NVARCHAR(20) NOT NULL UNIQUE DEFAULT 'UNKNOWN', -- DODANY CONSTRAINT UNIQUE
    bidding_zone_name NVARCHAR(100) NOT NULL DEFAULT 'Unknown Zone',
    primary_country NVARCHAR(50) NOT NULL DEFAULT 'Unknown',
    secondary_countries NVARCHAR(100) DEFAULT 'None',
    control_area NVARCHAR(50) NOT NULL DEFAULT 'Unknown',
    timezone NVARCHAR(30) NOT NULL DEFAULT 'UTC',
    population BIGINT DEFAULT 0,
    gdp_per_capita DECIMAL(12,2) DEFAULT 0.00,
    energy_intensity DECIMAL(10,2) DEFAULT 0.00,
    electricity_price_avg DECIMAL(10,2) DEFAULT 0.00
);

-- Wymiar stref pogodowych
CREATE TABLE dim_weather_zone (
    weather_zone_id INT IDENTITY(1,1) PRIMARY KEY,
    weather_zone_name NVARCHAR(100) NOT NULL DEFAULT 'Unknown Zone',
    bidding_zone_code NVARCHAR(20) NOT NULL DEFAULT 'UNKNOWN',
    climate_zone NVARCHAR(30) NOT NULL DEFAULT 'Unknown',
    elevation_avg DECIMAL(8,2) DEFAULT 0.00,
    coastal_proximity NVARCHAR(20) NOT NULL DEFAULT 'Unknown',
    urbanization_level NVARCHAR(20) NOT NULL DEFAULT 'Unknown',
    FOREIGN KEY (bidding_zone_code) REFERENCES dim_bidding_zone(bidding_zone_code)
);

-- Wymiar typów generacji energii
CREATE TABLE dim_generation_type (
    generation_type_id TINYINT IDENTITY(1,1) PRIMARY KEY,
    generation_category NVARCHAR(30) NOT NULL DEFAULT 'Unknown',
    generation_type NVARCHAR(30) NOT NULL DEFAULT 'Unknown',
    is_intermittent NVARCHAR(3) NOT NULL DEFAULT 'No',
    fuel_source NVARCHAR(30) NOT NULL DEFAULT 'Unknown'
);

-- Wymiar warunków pogodowych
CREATE TABLE dim_weather_condition (
    weather_condition_id SMALLINT IDENTITY(1,1) PRIMARY KEY,
    condition_type NVARCHAR(30) NOT NULL DEFAULT 'Normal',
    condition_severity NVARCHAR(30) NOT NULL DEFAULT 'Mild',
    is_extreme_weather NVARCHAR(3) NOT NULL DEFAULT 'No',
    extreme_weather_type NVARCHAR(30) NOT NULL DEFAULT 'None'
);

-- Wymiar profilu spo³eczno-ekonomicznego
CREATE TABLE dim_socioeconomic_profile (
    socioeconomic_profile_id TINYINT IDENTITY(1,1) PRIMARY KEY,
    profile_name NVARCHAR(50) NOT NULL DEFAULT 'Standard',
    bidding_zone_code NVARCHAR(20) NOT NULL DEFAULT 'UNKNOWN',
    avg_income_level DECIMAL(12,2) DEFAULT 0.00,
    unemployment_rate DECIMAL(3,2) DEFAULT 0.00,
    urbanization_rate DECIMAL(3,2) DEFAULT 0.00,
    service_sector_percentage DECIMAL(3,2) DEFAULT 0.00,
    industry_sector_percentage DECIMAL(3,2) DEFAULT 0.00,
    energy_poverty_rate DECIMAL(3,2) DEFAULT 0.00,
    residential_percentage DECIMAL(3,2) DEFAULT 0.00,
    commercial_percentage DECIMAL(3,2) DEFAULT 0.00,
    industrial_percentage DECIMAL(3,2) DEFAULT 0.00,
    avg_household_size TINYINT DEFAULT 0,
    primary_heating_type NVARCHAR(30) NOT NULL DEFAULT 'Unknown',
    FOREIGN KEY (bidding_zone_code) REFERENCES dim_bidding_zone(bidding_zone_code)
);

-- =======================================================
-- TABELA FAKTÓW
-- =======================================================

CREATE TABLE fact_energy_weather (
    energy_weather_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_id BIGINT NOT NULL DEFAULT 0,
    time_id BIGINT NOT NULL DEFAULT 0,
    bidding_zone_id TINYINT NOT NULL DEFAULT 0,
    weather_zone_id INT NOT NULL DEFAULT 0,
    generation_type_id TINYINT NOT NULL DEFAULT 0,
    socioeconomic_profile_id TINYINT NOT NULL DEFAULT 0,
    weather_condition_id SMALLINT NOT NULL DEFAULT 0,
    
    -- Miary energetyczne
    actual_consumption DECIMAL(15,2) DEFAULT 0.00,
    forecasted_consumption DECIMAL(15,2) DEFAULT 0.00,
    consumption_deviation DECIMAL(8,2) DEFAULT 0.00,
    generation_amount DECIMAL(15,2) DEFAULT 0.00,
    capacity_factor DECIMAL(6,2) DEFAULT 0.00,
    renewable_percentage DECIMAL(5,2) DEFAULT 0.00,
    per_capita_consumption DECIMAL(15,6) DEFAULT 0.00,
    
    -- Miary pogodowe
    temperature_min DECIMAL(5,2) DEFAULT -99.99,
    temperature_max DECIMAL(5,2) DEFAULT -99.99,
    temperature_avg DECIMAL(5,2) DEFAULT -99.99,
    precipitation DECIMAL(5,2) DEFAULT 0.00,
    wind_speed DECIMAL(5,2) DEFAULT 0.00,
    wind_direction INT DEFAULT 0,
    humidity DECIMAL(5,2) DEFAULT 0.00,
    solar_radiation DECIMAL(5,2) DEFAULT 0.00,
    air_pressure DECIMAL(5,2) DEFAULT 0.00,
    heating_degree_days DECIMAL(5,2) DEFAULT 0.00,
    cooling_degree_days DECIMAL(5,2) DEFAULT 0.00,
    
    -- Klucze obce
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (bidding_zone_id) REFERENCES dim_bidding_zone(bidding_zone_id),
    FOREIGN KEY (weather_zone_id) REFERENCES dim_weather_zone(weather_zone_id),
    FOREIGN KEY (generation_type_id) REFERENCES dim_generation_type(generation_type_id),
    FOREIGN KEY (socioeconomic_profile_id) REFERENCES dim_socioeconomic_profile(socioeconomic_profile_id),
    FOREIGN KEY (weather_condition_id) REFERENCES dim_weather_condition(weather_condition_id)
);

-- =======================================================
-- INDEKSY DLA OPTYMALIZACJI WYDAJNOŒCI
-- =======================================================

-- Indeksy na wymiarach
CREATE INDEX IX_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IX_dim_time_hour ON dim_time(hour);
CREATE INDEX IX_dim_bidding_zone_code ON dim_bidding_zone(bidding_zone_code);
CREATE INDEX IX_dim_weather_zone_code ON dim_weather_zone(bidding_zone_code);

-- Indeksy na tabeli faktów
CREATE INDEX IX_fact_energy_weather_date ON fact_energy_weather(date_id);
CREATE INDEX IX_fact_energy_weather_time ON fact_energy_weather(time_id);
CREATE INDEX IX_fact_energy_weather_zone ON fact_energy_weather(bidding_zone_id);
CREATE INDEX IX_fact_energy_weather_composite ON fact_energy_weather(date_id, time_id, bidding_zone_id);

-- =======================================================
-- TABELE POMOCNICZE I KONFIGURACYJNE
-- =======================================================

-- Tabela mapowania stref ENTSO-E
CREATE TABLE config_bidding_zones (
    eic_code NVARCHAR(20) PRIMARY KEY,
    zone_name NVARCHAR(100) NOT NULL,
    country_code NVARCHAR(5) NOT NULL,
    timezone NVARCHAR(30) NOT NULL,
    latitude DECIMAL(8,6),
    longitude DECIMAL(9,6)
);

-- Tabela mapowania typów generacji ENTSO-E
CREATE TABLE config_generation_types (
    entso_code NVARCHAR(5) PRIMARY KEY,
    generation_type NVARCHAR(50) NOT NULL,
    category NVARCHAR(30) NOT NULL,
    is_renewable BIT NOT NULL,
    is_intermittent BIT NOT NULL
);

-- Tabela logowania procesów ETL
CREATE TABLE etl_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    process_name NVARCHAR(100) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2,
    status NVARCHAR(20) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    records_processed INT DEFAULT 0,
    error_message NVARCHAR(MAX),
    execution_details NVARCHAR(MAX)
);

-- =======================================================
-- WSTAWIENIE DANYCH KONFIGURACYJNYCH
-- =======================================================

-- Mapowanie stref przetargowych ENTSO-E
INSERT INTO config_bidding_zones VALUES
('10YPL-AREA-----S', 'Poland', 'PL', 'Europe/Warsaw', 52.2297, 21.0122),
('10YDE-EON------1', 'Germany', 'DE', 'Europe/Berlin', 52.5200, 13.4050),
('10YFR-RTE------C', 'France', 'FR', 'Europe/Paris', 48.8566, 2.3522),
('10YES-REE------0', 'Spain', 'ES', 'Europe/Madrid', 40.4168, -3.7038),
('10YIT-GRTN-----B', 'Italy', 'IT', 'Europe/Rome', 41.9028, 12.4964);

-- Mapowanie typów generacji
INSERT INTO config_generation_types VALUES
('B01', 'Biomass', 'Renewable', 1, 0),
('B02', 'Fossil Brown coal/Lignite', 'Conventional', 0, 0),
('B03', 'Fossil Coal-derived gas', 'Conventional', 0, 0),
('B04', 'Fossil Gas', 'Conventional', 0, 0),
('B05', 'Fossil Hard coal', 'Conventional', 0, 0),
('B06', 'Fossil Oil', 'Conventional', 0, 0),
('B09', 'Geothermal', 'Renewable', 1, 0),
('B10', 'Hydro Pumped Storage', 'Renewable', 1, 0),
('B11', 'Hydro Run-of-river and poundage', 'Renewable', 1, 1),
('B12', 'Hydro Water Reservoir', 'Renewable', 1, 0),
('B13', 'Marine', 'Renewable', 1, 1),
('B14', 'Nuclear', 'Nuclear', 0, 0),
('B15', 'Other renewable', 'Renewable', 1, 1),
('B16', 'Solar', 'Renewable', 1, 1),
('B17', 'Waste', 'Other', 0, 0),
('B18', 'Wind Offshore', 'Renewable', 1, 1),
('B19', 'Wind Onshore', 'Renewable', 1, 1),
('B20', 'Other', 'Other', 0, 0);

GO

-- =======================================================
-- PROCEDURY POMOCNICZE
-- =======================================================

-- Procedura do upsert wymiarów
CREATE PROCEDURE sp_upsert_dimension
    @table_name NVARCHAR(100),
    @key_column NVARCHAR(100),
    @key_value NVARCHAR(100),
    @column_values NVARCHAR(MAX)
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = 'IF EXISTS (SELECT 1 FROM ' + @table_name + ' WHERE ' + @key_column + ' = ''' + @key_value + ''') 
                    UPDATE ' + @table_name + ' SET ' + @column_values + ' WHERE ' + @key_column + ' = ''' + @key_value + '''
                ELSE 
                    INSERT INTO ' + @table_name + ' (' + @key_column + ', ' + REPLACE(@column_values, ' = ', ', ') + ')';
    
    EXEC sp_executesql @sql;
END;
GO

-- Procedura logowania
CREATE PROCEDURE sp_log_etl_process
    @process_name NVARCHAR(100),
    @status NVARCHAR(20),
    @records_processed INT = 0,
    @error_message NVARCHAR(MAX) = NULL,
    @execution_details NVARCHAR(MAX) = NULL
AS
BEGIN
    IF @status = 'RUNNING'
    BEGIN
        INSERT INTO etl_log (process_name, start_time, status, records_processed, error_message, execution_details)
        VALUES (@process_name, GETDATE(), @status, @records_processed, @error_message, @execution_details);
    END
    ELSE
    BEGIN
        UPDATE etl_log 
        SET end_time = GETDATE(), 
            status = @status, 
            records_processed = @records_processed,
            error_message = @error_message,
            execution_details = @execution_details
        WHERE process_name = @process_name 
          AND status = 'RUNNING'
          AND log_id = (SELECT MAX(log_id) FROM etl_log WHERE process_name = @process_name AND status = 'RUNNING');
    END
END;
GO

PRINT 'Struktura bazy danych utworzona pomyœlnie!';