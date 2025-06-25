import azure.functions as func
import logging
import json
from datetime import datetime, timedelta
import os
import joblib  # モデルの読み込みに必要
import pandas as pd
import numpy as np
import requests

# Azure Functions アプリケーションのインスタンスを作成
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# --- グローバル変数としてモデルとその他の定数を定義 ---
# 関数アプリ起動時に一度だけ読み込まれる
CUSTOMER_MODELS = {} # 来客数・総杯数予測モデルを格納
BEER_MODELS = {}     # 各ビール販売量予測モデルを格納

OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
OPENWEATHER_CITY = os.environ.get("OPENWEATHER_CITY")

# モデル学習時に使われた特徴量リスト
BASE_FEATURES = ["平均気温(℃)", "曜日", "月", "天気(1-5)"]
FEATURE_COLS = ["来客数", "総杯数", "平均気温(℃)", "曜日", "月", "天気(1-5)"]

# 天気コードのマッピング
WEATHER_CODE_MAP = {
    "thunderstorm": 1, "drizzle": 1, "rain": 1, "light rain": 1,
    "moderate rain": 1, "shower rain": 1, "overcast clouds": 2,
    "broken clouds": 2, "scattered clouds": 3, "few clouds": 4,
    "clear sky": 5
}

# ビール販売量予測モデルの入力に必要なため、学習時の平均値を使う。
AVG_VISITORS = 13
AVG_CUPS = 22

# --- アプリケーション起動時にモデルを読み込むロジック ---
# このコードは関数アプリが起動する際に一度だけ実行される
try:
    model_base_path = os.path.join(os.path.dirname(__file__), "models")
    
    customer_model_files = {
        "来客数": "来客数_model.joblib",
        "総杯数": "総杯数_model.joblib"
    }
    for key, filename in customer_model_files.items():
        model_path = os.path.join(model_base_path, filename)
        if os.path.exists(model_path):
            CUSTOMER_MODELS[key] = joblib.load(model_path)
            logging.info(f"Loaded customer model: {key} from {model_path}")
        else:
            logging.warning(f"Customer model not found: {model_path}. Prediction for {key} might use averages.")

    # BEER_MODELS のキーは学習時のターゲット列名に合わせる (例: "IPA(本)")
    # modelsディレクトリ内の .joblib ファイルを動的に検出する
    for filename in os.listdir(model_base_path):
        if filename.endswith("_model.joblib") and not filename.startswith(("来客数_", "総杯数_")):
            model_path = os.path.join(model_base_path, filename)
            beer_key = filename.replace("_model.joblib", "").strip() + "(本)"
            BEER_MODELS[beer_key] = joblib.load(model_path)
            logging.info(f"Loaded beer model: {beer_key} from {model_path}")

except Exception as e:
    logging.error(f"Error loading models at application startup: {e}")

# --- HTTPトリガー関数 'get_order_recommendations' を定義 ---
@app.route(route="get_order_recommendations", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_order_recommendations(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for order recommendations.')

    # モデルが正常に読み込まれていない場合はエラーを返す
    if not CUSTOMER_MODELS or not BEER_MODELS:
        logging.error("Prediction models are not fully loaded. Cannot process request.")
        return func.HttpResponse(
            json.dumps({"error": "Prediction models are not ready. Please check application startup logs for errors."}),
            mimetype="application/json",
            status_code=500
        )

    # --- OpenWeather API から天気予報データを取得 ---
    # OpenWeatherMap APIの無料プランは5日間の予測（3時間ごと）を提供
    # ここでは最大5日分の予測データを集めることを試みる
    forecast_days_to_collect = 5
    
    forecast_data_list = []
    try:
        if not OPENWEATHER_API_KEY:
            raise ValueError("OpenWeather API Key is not configured.")

        weather_url = f"https://api.openweathermap.org/data/2.5/forecast?q={OPENWEATHER_CITY}&appid={OPENWEATHER_API_KEY}&units=metric&lang=ja"
        weather_response = requests.get(weather_url)
        weather_response.raise_for_status() # HTTPエラーがあれば例外を発生させる
        weather_data = weather_response.json()

        processed_dates = set()
        if weather_data.get("list"):
            for entry in weather_data["list"]:
                dt_obj = datetime.fromtimestamp(entry["dt"])
                current_date = dt_obj.date()

                # 正午のデータのみを使用し、まだ処理していない日付、かつ指定日数未満
                # かつ、日曜日でない（店舗は日曜日が定休日）
                if (dt_obj.hour == 12 and 
                    current_date not in processed_dates and 
                    len(processed_dates) < forecast_days_to_collect):
                    
                    if dt_obj.weekday() == 6: # 月=0, 日=6 (Sunday)
                        logging.info(f"Skipping Sunday (holiday): {current_date.strftime('%Y-%m-%d')}")
                        continue # 日曜日は定休日なので予測対象からスキップ

                    desc = entry["weather"][0]["description"].lower()
                    weather_code = WEATHER_CODE_MAP.get(desc, 3) # マッピングにない場合は'曇り' (3) とする

                    forecast_data_list.append({
                        "日付": current_date, # datetime.dateオブジェクトのまま保持
                        "平均気温(℃)": entry["main"]["temp"],
                        "曜日": dt_obj.weekday(),
                        "月": dt_obj.month,
                        "天気(1-5)": weather_code
                    })
                    processed_dates.add(current_date)
                
                # 必要な日数分のデータを取得したらループを終了
                if len(processed_dates) >= forecast_days_to_collect:
                    break

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data from OpenWeather API: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to fetch weather data: {str(e)}."}),
            mimetype="application/json",
            status_code=500
        )
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Server configuration error: {str(e)}"}),
            mimetype="application/json",
            status_code=500
        )
    except Exception as e:
        logging.error(f"Error processing weather data: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"An unexpected error occurred while processing weather data: {str(e)}."}),
            mimetype="application/json",
            status_code=500
        )

    if not forecast_data_list:
        logging.error("No valid weather forecast data could be retrieved for the specified period.")
        return func.HttpResponse(
            json.dumps({"error": "No valid forecast data available for calculation. Please check OpenWeather API configuration."}),
            mimetype="application/json",
            status_code=404
        )

    # Pandas DataFrameに変換して予測の準備
    daily_forecast_df = pd.DataFrame(forecast_data_list)

    # --- 来客数・総杯数予測 ---
    # customer_models が正常に読み込まれていればそれを使用、そうでなければ学習時の平均値を使用
    if "来客数" in CUSTOMER_MODELS:
        daily_forecast_df["来客数"] = np.round(CUSTOMER_MODELS["来客数"].predict(daily_forecast_df[BASE_FEATURES])).astype(int)
    else:
        logging.warning("来客数 prediction model not loaded. Using average for 来客数.")
        daily_forecast_df["来客数"] = AVG_VISITORS
    
    if "総杯数" in CUSTOMER_MODELS:
        daily_forecast_df["総杯数"] = np.round(CUSTOMER_MODELS["総杯数"].predict(daily_forecast_df[BASE_FEATURES])).astype(int)
    else:
        logging.warning("総杯数 prediction model not loaded. Using average for 総杯数.")
        daily_forecast_df["総杯数"] = AVG_CUPS

    # --- 各ビール販売数予測を実行し、daily_forecast_df に 'predicted_beers' 列として追加 ---
    # このステップで、各日付ごとの予測されたビール販売量を辞書形式で格納する新しい列を作成。
    # 発注量計算時にこの列から各ビールの予測量を参照する。
    
    # 予測対象となる全てのビールタイプ (例: "IPA(本)", "Lager(本)", ...)
    all_beer_types = list(BEER_MODELS.keys())
    
    # 各日付ごとにビールの予測を実行し、'predicted_beers'列に辞書として格納
    daily_forecast_df['predicted_beers'] = daily_forecast_df.apply(
        lambda row: {
            beer_key_full: max(0, int(np.round(BEER_MODELS[beer_key_full].predict(pd.DataFrame([row[FEATURE_COLS]])))))
            if beer_key_full in BEER_MODELS else 0
            for beer_key_full in all_beer_types # 全ての学習済みビールタイプに対して予測を試みる
        }, axis=1
    )


    # --- 発注量計算（週2回: 月・木） ---
    order_recommendations_output = []

    # 今日の日付を基準に、次の月曜日と木曜日を計算
    current_today_date = datetime.now().date()
    next_monday_order_day = current_today_date + timedelta(days=(0 - current_today_date.weekday() + 7) % 7)
    next_thursday_order_day = current_today_date + timedelta(days=(3 - current_today_date.weekday() + 7) % 7)
    
    # 発注量計算ヘルパー関数
    # この関数は、指定された日付範囲内のビールの合計予測量を計算
    # daily_forecast_df には既に日曜日が除外されたデータと、予測結果の 'predicted_beers' 列が存在する前提
    def calculate_order_period_sum(start_date, end_date):
        period_df = daily_forecast_df[
            (daily_forecast_df["日付"] >= start_date) & 
            (daily_forecast_df["日付"] <= end_date)
        ]
        
        period_sums = {}
        if not period_df.empty:
            for beer_key_full in all_beer_types:
                # 'predicted_beers' 列の辞書から各ビールの予測量を取得し、合計
                total_quantity = period_df["predicted_beers"].apply(lambda x: x.get(beer_key_full, 0)).sum()
                period_sums[beer_key_full] = int(total_quantity) # 整数に変換
        return period_sums

    # 月曜日発注分 (火〜木曜日分の予測を合計)
    # 翌日納品なので、月曜に発注→火曜着。火・水・木曜日分の需要をカバー
    monday_order_start = next_monday_order_day + timedelta(days=1)
    monday_order_end = next_monday_order_day + timedelta(days=3)
    monday_order_sums = calculate_order_period_sum(monday_order_start, monday_order_end)
    if monday_order_sums:
        order_recommendations_output.append({
            "order_date": next_monday_order_day.strftime("%Y-%m-%d"),
            "order_day_label": "月",
            "coverage_period_start": monday_order_start.strftime("%Y-%m-%d"),
            "coverage_period_end": monday_order_end.strftime("%Y-%m-%d"),
            "ordered_beers": monday_order_sums
        })

    # 木曜日発注分 (金〜翌月曜日分の予測を合計)
    # 翌日納品なので、木曜に発注→金曜着。金・土・月曜日分の需要をカバー（日曜は定休日なので予測データにない前提）
    thursday_order_start = next_thursday_order_day + timedelta(days=1)
    thursday_order_end = next_thursday_order_day + timedelta(days=4)
    thursday_order_sums = calculate_order_period_sum(thursday_order_start, thursday_order_end)
    if thursday_order_sums:
        order_recommendations_output.append({
            "order_date": next_thursday_order_day.strftime("%Y-%m-%d"),
            "order_day_label": "木",
            "coverage_period_start": thursday_order_start.strftime("%Y-%m-%d"),
            "coverage_period_end": thursday_order_end.strftime("%Y-%m-%d"),
            "ordered_beers": thursday_order_sums
        })

    # 最終レスポンスデータには「週2回（月・木）の発注推奨量」のみを含める
    final_response = {
        "order_recommendations": order_recommendations_output,
        "unit": "本", # 予測量の単位
        "model_info": "Calculated based on current date and forecasted data."
    }

    # JSONレスポンスを返す
    return func.HttpResponse(
        json.dumps(final_response, ensure_ascii=False, indent=2), # ensure_ascii=Falseで日本語文字化けを防ぐ, indentで整形
        mimetype="application/json",
        status_code=200
    )

