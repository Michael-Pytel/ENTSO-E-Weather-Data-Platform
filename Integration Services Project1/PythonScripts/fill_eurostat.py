country_code = record['country_code']
        
        # Mapowanie domyślnych wartości do użycia, gdy brak danych z Eurostatu
        default_values = {
            'poverty_by_degree_of_urbanization': {
                'PL': 60.0, 'DE': 77.0, 'FR': 81.0, 'ES': 80.0, 'IT': 70.0,
                'CZ': 74.0, 'SK': 54.0, 'HU': 72.0
            },
            'service_sector_percentage': {
                'PL': 65.0, 'DE': 69.0, 'FR': 79.0, 'ES': 76.0, 'IT': 74.0,
                'CZ': 60.0, 'SK': 62.0, 'HU': 65.0
            },
            'industry_sector_percentage': {
                'PL': 33.0, 'DE': 29.0, 'FR': 19.0, 'ES': 23.0, 'IT': 24.0,
                'CZ': 37.0, 'SK': 35.0, 'HU': 31.0
            },
            'avg_household_size': {
                'PL': 2.6, 'DE': 2.0, 'FR': 2.2, 'ES': 2.5, 'IT': 2.3,
                'CZ': 2.4, 'SK': 2.6, 'HU': 2.3
            },
            'primary_heating_type': {
                'PL': 'District Heating', 'DE': 'Natural Gas', 'FR': 'Electricity',
                'ES': 'Natural Gas', 'IT': 'Natural Gas', 'CZ': 'District Heating',
                'SK': 'Natural Gas', 'HU': 'Natural Gas'
            }
        }
        
        # Uzupełnianie brakujących wartości tylko gdy są None
        if record['poverty_by_degree_of_urbanization'] is None:
            record['poverty_by_degree_of_urbanization'] = default_values['poverty_by_degree_of_urbanization'].get(country_code, 65.0)
        
        if record['service_sector_percentage'] is None:
            record['service_sector_percentage'] = default_values['service_sector_percentage'].get(country_code, 68.0)
        
        if record['industry_sector_percentage'] is None:
            record['industry_sector_percentage'] = default_values['industry_sector_percentage'].get(country_code, 30.0)
        
        if record['avg_household_size'] is None:
            record['avg_household_size'] = default_values['avg_household_size'].get(country_code, 2.5)
        
        if record['primary_heating_type'] is None:
            record['primary_heating_type'] = default_values['primary_heating_type'].get(country_code, 'Natural Gas')
        
        # Ubóstwo energetyczne szacowane na podstawie PKB per capita, jeśli brak danych
        if record['energy_poverty_rate'] is None and record['gdp_per_capita'] is not None:
            gdp = record['gdp_per_capita']
            if gdp > 40000:
                record['energy_poverty_rate'] = 3.0
            elif gdp > 25000:
                record['energy_poverty_rate'] = 8.0
            elif gdp > 15000:
                record['energy_poverty_rate'] = 15.0
            else:
                record['energy_poverty_rate'] = 25.0