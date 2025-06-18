import requests
import json
 
API_KEY = "8cc8c00d65ab167e9c1bb014958a878e"
CITY = "Tokyo"
URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric&lang=ja"
 
response = requests.get(URL)
data = response.json()
 
# 五日
with open("weather_5days.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
 
print("天気資料 weather_5days.json")
