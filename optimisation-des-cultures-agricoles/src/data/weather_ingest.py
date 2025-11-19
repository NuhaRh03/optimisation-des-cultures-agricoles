import requests
import json
from datetime import datetime

# 1. إعدادات API (استخدم مفتاحك الخاص)
# ملاحظة: يجب استبدالها بـ API مفضلة في مرحلة الإنتاج (مثل DMN أو API تجارية زراعية)
API_KEY = "VOTRE_CLE_API"  # استبدلها بمفتاحك
LATITUDE = 31.629  # خط العرض (مراكش)
LONGITUDE = -8.008  # خط الطول (مراكش)
API_URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LATITUDE}&lon={LONGITUDE}&appid={API_KEY}&units=metric"

# 2. جلب البيانات الجوية الحالية
def fetch_current_weather():
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # إلقاء خطأ إذا كان الرد غير ناجح
        data = response.json()

        # استخلاص المعلمات الزراعية الرئيسية
        weather_record = {
            'parcel_lat': LATITUDE,
            'parcel_lon': LONGITUDE,
            'timestamp': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'temp_c': data['main']['temp'],          # درجة الحرارة (°C)
            'humidity_pct': data['main']['humidity'], # الرطوبة (%)
            'wind_speed_m_s': data['wind']['speed'],  # سرعة الرياح (م/ث)
            'pressure_hPa': data['main']['pressure'],  # الضغط الجوي (هكتوباسكال)
            # يجب جمع بيانات الأمطار والإشعاع من API متقدمة لتكون أكثر فائدة
        }
        return weather_record
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

# 3. تجهيز البيانات للاستيعاب في Big Data
current_weather = fetch_current_weather()

if current_weather:
    # 4. تخزين النتائج في ملف JSON مؤقت (جاهز للاستيعاب)
    with open('current_weather.json', 'w') as f:
        json.dump(current_weather, f, indent=4)
    print("Current weather data exported to current_weather.json, ready for ingestion.")