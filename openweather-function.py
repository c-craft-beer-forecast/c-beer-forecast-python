import logging
import azure.functions as func
import os
import requests
import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta

app = func.FunctionApp()

# --- 環境変数から設定を取得 ---
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
OPENWEATHER_CITY = os.environ.get("OPENWEATHER_CITY")

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

# --- 天気コードのマッピング (beer-forecast-model-functionと同じもの) ---
WEATHER_CODE_MAP = {
    "thunderstorm": 1, "drizzle": 1, "rain": 1, "light rain": 1,
    "moderate rain": 1, "shower rain": 1, "overcast clouds": 2,
    "broken clouds": 2, "scattered clouds": 3, "few clouds": 4,
    "clear sky": 5
}

# --- 曜日のマッピング (Pythonのweekday()は月曜日=0, 日曜日=6) ---
WEEKDAY_MAP = {
    0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"
}

@app.timer_trigger(schedule="0 0 12 * * *", arg_name="myTimer", run_on_startup=False,
                   use_monitor=False)
def collect_weather_data(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().isoformat()
    logging.info('Python timer trigger function started at %s (UTC)', utc_timestamp)

    if myTimer.past_due:
        logging.info('The timer is past due!')

    # --- 環境変数チェック ---
    if not all([OPENWEATHER_API_KEY, OPENWEATHER_CITY, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        logging.error("Required environment variables (OpenWeather API/DB credentials) are not set. Exiting.")
        return

    # 今日の日付を取得 (Azure Functionsのタイムゾーン設定に依存)
    # WEBSITE_TIME_ZONE が 'Tokyo Standard Time' に設定されていればJSTになります
    today_date = datetime.now().date()
    logging.info(f"Attempting to collect weather data for: {today_date.strftime('%Y-%m-%d')}")

    # --- OpenWeather API から今日の正午の天気データを取得 ---
    target_weather_data = None
    try:
        weather_url = f"https://api.openweathermap.org/data/2.5/forecast?q={OPENWEATHER_CITY}&appid={OPENWEATHER_API_KEY}&units=metric&lang=ja"
        weather_response = requests.get(weather_url)
        weather_response.raise_for_status() # HTTPエラー（4xx, 5xx）があれば例外を発生
        weather_data = weather_response.json()

        if weather_data.get("list"):
            for entry in weather_data["list"]:
                dt_obj = datetime.fromtimestamp(entry["dt"])
                # その日の正午のデータを探す
                if dt_obj.date() == today_date and dt_obj.hour == 12:
                    desc = entry["weather"][0]["description"].lower()
                    weather_code = WEATHER_CODE_MAP.get(desc, 3) # マッピングにない場合は'曇り' (3) とする
                    
                    target_weather_data = {
                        "record_date": today_date,
                        "avg_temp_c": entry["main"]["temp"],
                        "day_of_week": WEEKDAY_MAP.get(dt_obj.weekday(), "不明"),
                        "month": dt_obj.month,
                        "weather_code": weather_code
                    }
                    logging.info(f"Successfully retrieved weather data for {today_date}: {target_weather_data}")
                    break
            
            if target_weather_data is None:
                logging.warning(f"No noon weather forecast found for {today_date} from OpenWeather API. Skipping DB save.")
                return

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data from OpenWeather API: {e}. Exiting.")
        return
    except Exception as e:
        logging.error(f"An unexpected error occurred during weather data retrieval: {e}. Exiting.")
        return

    # --- PostgreSQL データベースに保存 ---
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO daily_weather_data (record_date, avg_temp_c, day_of_week, month, weather_code)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (record_date) DO UPDATE SET
                avg_temp_c = EXCLUDED.avg_temp_c,
                day_of_week = EXCLUDED.day_of_week,
                month = EXCLUDED.month,
                weather_code = EXCLUDED.weather_code;
        """
        data_to_insert = (
            target_weather_data["record_date"],
            target_weather_data["avg_temp_c"],
            target_weather_data["day_of_week"],
            target_weather_data["month"],
            target_weather_data["weather_code"]
        )

        cursor.execute(insert_sql, data_to_insert)
        conn.commit()
        logging.info(f"Successfully saved weather data for {target_weather_data['record_date']} to PostgreSQL.")

    except (Exception, Error) as e:
        logging.error(f"Error saving data to PostgreSQL: {e}")
        if conn:
            conn.rollback() # エラーが発生した場合はロールバック
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    logging.info('Python timer trigger function completed.')
