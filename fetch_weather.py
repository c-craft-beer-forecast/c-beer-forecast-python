import requests
import json
 
API_KEY = "8cc8c00d65ab167e9c1bb014958a878e"
CITY = "Tokyo"
UNITS = "metric"  # 摂氏を使用
LANG = "ja"       # 日本語の天気説明
# === ステップ2: OpenWeather APIにリクエストを送信 ===
url = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units={UNITS}&lang={LANG}"
response = requests.get(url)
 
# === ステップ3: レスポンスを処理・整形 ===
if response.status_code == 200:
    data = response.json()
    forecasts = data.get("list", [])
 
    results = []
 
    for entry in forecasts:
        dt_txt = entry.get("dt_txt", "")  # 日時（文字列）
        weather_main = entry["weather"][0]["main"]          # 天気の種類（例：Clear, Rainなど）
        weather_desc = entry["weather"][0]["description"]   # 天気の詳細（日本語）
        temp = entry["main"]["temp"]                        # 気温（摂氏）
 
        results.append({
            "日時": dt_txt,
            "天気": weather_desc,
            "気温（℃）": temp
        })
 
    # === ステップ4: 結果をJSONファイルとして保存 ===
    with open("weather_5days.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
 
    print("✅ 5日間の天気情報を正常に取得しました。weather_5days.json に保存しました。")
 
else:
    print(f"❌ APIリクエストに失敗しました。HTTPステータスコード：{response.status_code}")