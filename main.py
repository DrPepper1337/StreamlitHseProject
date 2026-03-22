import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import datetime
from parallelDataAnalysis import analyse_city_data
from asyncApiExecution import wrapper
import asyncio


if __name__ == "__main__":
    st.title('⛅Анализ температуры городов')

    uploaded_file = st.file_uploader("Загрузите файл с погодными данными c разрешением csv", accept_multiple_files=False, type='csv')
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file, parse_dates=['timestamp'])
        df = df.sort_values(['city', 'timestamp'])
        result_df = pd.concat([analyse_city_data(group) for _, group in df.groupby('city')], ignore_index=True)
        city = st.selectbox(
            "Выбирите город",
            df['city'].unique(),
        )
        city_data = result_df[result_df["city"] == city].copy()

        st.subheader(f"Исторические данные по сезонам в городе {city}")

        seasonal_stats = (
            city_data.groupby("season")["temperature"]
            .agg(["mean", "std", "min", "max"])
            .reindex(["winter", "spring", "summer", "autumn"])
            .reset_index()
        )

        seasonal_stats.columns = [
            "Сезон",
            "Средняя температура",
            "Стандартное отклонение",
            "Минимум",
            "Максимум"
        ]

        st.dataframe(seasonal_stats, width='stretch', hide_index=True)

        st.subheader(f"График температур города {city}")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=city_data["timestamp"],
                y=city_data["temperature"],
                mode="lines",
                name="Температура",
            )
        )

        fig.add_trace(
            go.Scatter(
                x=city_data["timestamp"],
                y=city_data["rolling_mean"],
                mode="lines",
                name="Скользящее среднее за 30 дней",
            )
        )

        anomalies = city_data[city_data["anomaly"]]

        fig.add_trace(
            go.Scatter(
                x=anomalies["timestamp"],
                y=anomalies["temperature"],
                mode="markers",
                name="Аномальные значения",
                marker=dict(size=5)
            )
        )

        fig.update_layout(
            xaxis_title="Дата",
            yaxis_title="Температура",
            hovermode="x unified"
        )

        st.plotly_chart(fig, width='stretch')

        st.subheader("Информация о текущей температуре")
        
        api_key = st.text_input("Введите ваш API ключ от OpenWeatherMap", type="password")
        if api_key != "":
            resp = asyncio.run(wrapper(city, api_key))
            if resp['cod'] != 200:
                st.error(resp)
            else:
                season = ["winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer", "autumn", "autumn", "autumn", "winter"][datetime.datetime.now().month - 1]
                lower_bound = result_df[(result_df["season"] == season) & (result_df["city"] == city)].iloc[0]['lower_bound']
                upper_bound = result_df[(result_df["season"] == season) & (result_df["city"] == city)].iloc[0]['upper_bound']
                cols = st.columns(2)
                season = ["winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer", "autumn", "autumn", "autumn", "winter"][datetime.datetime.now().month - 1]
                cols[0].metric("Текущая температура", f"{resp['main']['temp']} C")
                cols[1].metric("Средняя температура за текущее время года", f"{result_df[(result_df["season"] == season) & (result_df["city"] == city)].iloc[0]['mean']:.2f} C")
                if lower_bound <= resp['main']['temp'] <= upper_bound:
                    st.success("Текущая температура является нормальной для этого города в это время года!", icon="✅")
                else:
                    st.error("Текущая температура является аномальной для этого города в это времягода!", icon="❌")
                st.metric("Доверительный интервал нормальности температуры", f"От {lower_bound:.2f} C до {upper_bound:.2f} C")
