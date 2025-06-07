"""
Skrypt do analizy odpowiedzi API ENTSO-E
Pobiera i zapisuje odpowiedź API do pliku XML
"""

import requests
import xml.etree.ElementTree as ET
import sys
import logging

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ENTSO-E_Analyzer")

def save_api_response(token, output_file="entsoe_response.xml"):
    """Pobiera i zapisuje odpowiedź API do pliku"""
    # Parametry zapytania - użyj krótszego zakresu dat z 2023 roku
    params = {
        'securityToken': token,
        'documentType': 'A65',  # Actual total load
        'processType': 'A16',   # Realised
        'outBiddingZone_Domain': '10YPL-AREA-----S',  # Poland
        'periodStart': '202304010000',  # 1 kwietnia 2023
        'periodEnd': '202304020000'     # 2 kwietnia 2023
    }
    
    logger.info(f"Making request to ENTSO-E API with parameters: {params}")
    
    try:
        response = requests.get("https://web-api.tp.entsoe.eu/api", params=params, timeout=30)
        
        logger.info(f"Response status: {response.status_code}")
        if response.status_code == 200:
            # Zapisz do pliku
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.info(f"Response saved to {output_file} ({len(response.text)} bytes)")
            return True
        else:
            logger.error(f"API Error: {response.status_code} - {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Request Error: {str(e)}")
        return False

def analyze_xml_structure(xml_file):
    """Analizuje i wyświetla strukturę pliku XML"""
    try:
        logger.info(f"Analyzing XML structure in {xml_file}")
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Wypisz główny tag i jego przestrzeń nazw
        if '}' in root.tag:
            namespace = root.tag.split('}')[0] + '}'
            tag_name = root.tag.split('}')[1]
            logger.info(f"Root element: {tag_name}")
            logger.info(f"Namespace: {namespace[1:-1]}")
        else:
            logger.info(f"Root element: {root.tag}")
            logger.info("No namespace found")
        
        # Znajdź i wypisz główne elementy (dzieci korzenia)
        logger.info("\nMain elements (direct children of root):")
        for i, child in enumerate(root):
            if i < 10:  # Pokaż tylko pierwsze 10 elementów
                tag = child.tag.split('}')[1] if '}' in child.tag else child.tag
                logger.info(f"- {tag}")
            else:
                logger.info(f"... and {len(list(root)) - 10} more elements")
                break
        
        # Szukaj ważnych elementów
        logger.info("\nSearching for key elements:")
        search_elements = ["TimeSeries", "Series_Period", "Point", "Period"]
        
        for elem in search_elements:
            # Szukaj elementu w dowolnej przestrzeni nazw
            found_elements = root.findall(f".//*{elem}") + root.findall(f".//{elem}")
            logger.info(f"Found {len(found_elements)} '{elem}' elements")
            
            # Pokaż przykład pierwszego elementu, jeśli znaleziono
            if found_elements:
                sample = found_elements[0]
                logger.info(f"Sample {elem} element:")
                # Pokaż dzieci przykładowego elementu
                for i, child in enumerate(sample):
                    if i < 5:  # Pokaż tylko pierwsze 5 dzieci
                        child_tag = child.tag.split('}')[1] if '}' in child.tag else child.tag
                        child_text = child.text.strip() if child.text else ""
                        if len(child_text) > 30:
                            child_text = child_text[:27] + "..."
                        logger.info(f"  - {child_tag}: {child_text}")
                    else:
                        logger.info(f"  ... and {len(list(sample)) - 5} more child elements")
                        break
        
        logger.info("\nAnalysis complete")
        return True
    
    except Exception as e:
        logger.error(f"Error analyzing XML: {str(e)}")
        return False

def main():
    """Główna funkcja"""
    if len(sys.argv) < 2:
        logger.error("Please provide your ENTSO-E API token as command line argument")
        logger.error("Usage: python analyze_entsoe_response.py YOUR_API_TOKEN")
        return
    
    token = sys.argv[1]
    output_file = "entsoe_response.xml"
    
    if save_api_response(token, output_file):
        analyze_xml_structure(output_file)

if __name__ == "__main__":
    main()