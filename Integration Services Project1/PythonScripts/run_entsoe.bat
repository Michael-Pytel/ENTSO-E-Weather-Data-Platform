@echo off
setlocal EnableDelayedExpansion

rem Ustaw katalog logowania
set LOG_DIR=D:\temp
if not exist %LOG_DIR% mkdir %LOG_DIR%

echo [%date% %time%] Start skryptu > %LOG_DIR%\run_entsoe_full.log
echo [%date% %time%] Parametry: %* >> %LOG_DIR%\run_entsoe_full.log

rem Pobierz parametry
set ENTSO_TOKEN=%~1
set DW_CONNECTION_STRING=%~2
set START_DATE=%~3
set END_DATE=%~4

rem Konwersja dat do formatu 2023 roku
set START_YEAR=%START_DATE:~0,4%
set START_YEAR_INT=%START_YEAR%
if %START_YEAR_INT% GEQ 2024 (
    set NEW_START_DATE=2023%START_DATE:~4%
    echo [%date% %time%] Zmieniono datę początkową z %START_DATE% na %NEW_START_DATE% >> %LOG_DIR%\run_entsoe_full.log
    set START_DATE=%NEW_START_DATE%
)

set END_YEAR=%END_DATE:~0,4%
set END_YEAR_INT=%END_YEAR%
if %END_YEAR_INT% GEQ 2024 (
    set NEW_END_DATE=2023%END_DATE:~4%
    echo [%date% %time%] Zmieniono datę końcową z %END_DATE% na %NEW_END_DATE% >> %LOG_DIR%\run_entsoe_full.log
    set END_DATE=%NEW_END_DATE%
)

echo [%date% %time%] Token: %ENTSO_TOKEN% >> %LOG_DIR%\run_entsoe_full.log
echo [%date% %time%] Connection: %DW_CONNECTION_STRING% >> %LOG_DIR%\run_entsoe_full.log
echo [%date% %time%] Start date: %START_DATE% >> %LOG_DIR%\run_entsoe_full.log
echo [%date% %time%] End date: %END_DATE% >> %LOG_DIR%\run_entsoe_full.log

rem Pełne ścieżki
set PYTHON_EXE=C:\Users\jakub\anaconda3\python.exe
set SCRIPT_DIR=D:\hurtownie\Integration Services Project1\PythonScripts
set SCRIPT_PATH=%SCRIPT_DIR%\ENTSOEClient.py

rem Sprawdź, czy plik Python istnieje
echo [%date% %time%] Sprawdzanie, czy skrypt Python istnieje... >> %LOG_DIR%\run_entsoe_full.log
if exist "%SCRIPT_PATH%" (
    echo [%date% %time%] Plik %SCRIPT_PATH% znaleziony >> %LOG_DIR%\run_entsoe_full.log
) else (
    set SCRIPT_PATH=%SCRIPT_DIR%\ENSTOEClient.py
    if exist "%SCRIPT_PATH%" (
        echo [%date% %time%] Znaleziono %SCRIPT_PATH% >> %LOG_DIR%\run_entsoe_full.log
    ) else (
        echo [%date% %time%] BŁĄD: Nie znaleziono pliku Python >> %LOG_DIR%\run_entsoe_full.log
        echo [%date% %time%] Zawartość katalogu %SCRIPT_DIR%: >> %LOG_DIR%\run_entsoe_full.log
        dir /b "%SCRIPT_DIR%" >> %LOG_DIR%\run_entsoe_full.log
        exit /b 1
    )
)

rem Sprawdź, czy Python istnieje
if not exist "%PYTHON_EXE%" (
    echo [%date% %time%] BŁĄD: Nie znaleziono interpretera Python: %PYTHON_EXE% >> %LOG_DIR%\run_entsoe_full.log
    echo [%date% %time%] Próba znalezienia Python w PATH... >> %LOG_DIR%\run_entsoe_full.log
    where python >> %LOG_DIR%\run_entsoe_full.log 2>&1
    set PYTHON_EXE=python
)

rem Składanie komendy Python
set PYTHON_CMD="%PYTHON_EXE%" "%SCRIPT_PATH%" --token %ENTSO_TOKEN% --connection "%DW_CONNECTION_STRING%" --start %START_DATE% --end %END_DATE%
echo [%date% %time%] Komenda Python: %PYTHON_CMD% >> %LOG_DIR%\run_entsoe_full.log

rem Uruchomienie Python i zapisanie wyjścia
echo [%date% %time%] Uruchamianie skryptu Python... >> %LOG_DIR%\run_entsoe_full.log
%PYTHON_CMD% > "%LOG_DIR%\python_output.log" 2> "%LOG_DIR%\python_error.log"
set PYTHON_EXIT_CODE=%ERRORLEVEL%

echo [%date% %time%] Kod wyjścia Python: %PYTHON_EXIT_CODE% >> %LOG_DIR%\run_entsoe_full.log

rem Dołącz wyjście i błędy Python do logu
echo [%date% %time%] Standardowe wyjście Python: >> %LOG_DIR%\run_entsoe_full.log
type "%LOG_DIR%\python_output.log" >> %LOG_DIR%\run_entsoe_full.log
echo. >> %LOG_DIR%\run_entsoe_full.log
echo [%date% %time%] Standardowe błędy Python: >> %LOG_DIR%\run_entsoe_full.log
type "%LOG_DIR%\python_error.log" >> %LOG_DIR%\run_entsoe_full.log
echo. >> %LOG_DIR%\run_entsoe_full.log

rem Koniec skryptu
echo [%date% %time%] Koniec skryptu, zwracanie kodu: %PYTHON_EXIT_CODE% >> %LOG_DIR%\run_entsoe_full.log
exit /b %PYTHON_EXIT_CODE%