import pandas as pd
import datetime
import asyncio
import requests
import time
from parallelDataAnalysis import analyse_city_data


def get_current_temp(city, api_key):
    return requests.get(f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric").json()

async def wrapper(city, api_key):
    return await asyncio.to_thread(get_current_temp, city, api_key)

if __name__ == "__main__":
    api_key = input("Введите API Key OpenWeatherMap: ")
    city = input("Введите город: ")

    df = pd.read_csv("temperature_data.csv", parse_dates=["timestamp"]).sort_values(["city", "timestamp"])
    result_df = pd.concat([analyse_city_data(group) for _, group in df.groupby("city")],ignore_index=True)

    start = time.time()
    current_temp = get_current_temp(city, api_key)
    if current_temp['cod'] != 200:
        print(current_temp)
        exit(-1)
    end = time.time()
    print("\nСинхронный запрос:")
    print(f"Время выполнения: {end - start:.4f}")

    start = time.time()
    current_temp = asyncio.run(wrapper(city, api_key))
    end = time.time()
    print("\nАсинхронный запрос:")
    print(f"Время выполнения: {end - start:.4f}")

    current_temp = current_temp['main']['temp']
    season = ["winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer", "autumn", "autumn", "autumn", "winter"][datetime.datetime.now().month - 1]
    result = result_df[(result_df["city"] == city) &(result_df["season"] == season)].iloc[0]
    print(f"Город: {city}")
    print(f"Текущий сезон: {season}")
    print(f"Текущая температура: {current_temp:.2f} C")
    print(f"Средняя историческая температура: {result['mean']:.2f} C")
    print(f"Нормальный диапазон: от {result['lower_bound']:.2f} C до {result['upper_bound']:.2f} C")

    if result['lower_bound'] <= current_temp <= result['upper_bound']:
        print("Текущая температура является нормальной")
    else:
        print("Текущая температура является аномальной")