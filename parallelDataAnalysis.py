import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import time

def analyse_city_data(city_df):
    city_df = city_df.sort_values('timestamp').copy()

    city_df['rolling_mean'] = (
        city_df['temperature'].rolling(window=30, min_periods=1).mean()
    )
    city_df = city_df.join(city_df.groupby('season')['temperature'].agg(['mean', 'std']), on='season')

    city_df['lower_bound'] = city_df['mean'] - 2 * city_df['std']
    city_df['upper_bound'] = city_df['mean'] + 2 * city_df['std']

    city_df['anomaly'] = (
        (city_df['temperature'] < city_df['lower_bound']) |
        (city_df['temperature'] > city_df['upper_bound'])
    )

    return city_df


if __name__ == "__main__":
    df = pd.read_csv('temperature_data.csv')
    n = 100
    sum_seq = 0
    sum_par = 0
    groups = [g for _, g in df.groupby('city')]

    for i in range(n):
        start = time.perf_counter()
        seq = pd.concat([analyse_city_data(g) for g in groups], ignore_index=True)
        seq_time = time.perf_counter() - start

        start = time.perf_counter()
        with ProcessPoolExecutor() as ex:
            par = pd.concat(list(ex.map(analyse_city_data, groups)), ignore_index=True)
        par_time = time.perf_counter() - start

        sum_seq += seq_time
        sum_par += par_time

        print(f"Номер эксперимента {i + 1}")
        print(f"Время последовательного выполнения: {seq_time}")
        print(f"Время параллельного выполнения: {par_time}")

    print("\nИТОГИ:")
    print(f"Среднее время последовательного выполнения: {sum_seq / n}")
    print(f"Среднее время параллельного выполнения: {sum_par / n}")