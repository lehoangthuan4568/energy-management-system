@echo off
echo ====================================================
echo      ENERGY MANAGEMENT SYSTEM - STARTUP SCRIPT
echo ====================================================
echo.
echo [1/3] Khoi dong Producer (Gia lap du lieu)...
start "EMS - Producer (Data Simulator) [Nhan A de tao canh bao]" cmd /k "python producer\producer.py"

echo [2/3] Doi 3 giay truoc khi khoi dong Spark Streaming...
timeout /t 3 /nobreak >nul
start "EMS - Spark Streaming Processor" cmd /k "python run_spark.py"

echo [3/3] Doi 5 giay de Spark xay dung DB roi chay AI Du Bao...
timeout /t 5 /nobreak >nul
start "EMS - AI Forecast (Holt-Winters)" cmd /k "python prediction\predict_power.py"

echo.
echo ====================================================
echo HOAN TAT! Cac Module dang chay trong 3 cua so moi.
echo De xem Dashboard, mo trinh duyet: http://localhost:3000
echo ====================================================
echo Nhan phim bat ky de dong cua so nay...
pause >nul
