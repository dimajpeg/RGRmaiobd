import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from loguru import logger
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError


# Функция для подключения к ZooKeeper
def connect_to_zookeeper():
    try:
        # Подключение к ZooKeeper
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()
        logger.info("Соединение с ZooKeeper установлено")

        # Создание узла в ZooKeeper
        path = "/big_data_node"
        if zk.exists(path):
            logger.info(f"Узел {path} уже существует")
        else:
            zk.create(path, b"Initial data")
            logger.info(f"Создан узел: {path}")

        return zk
    except Exception as e:
        logger.error(f"Ошибка при подключении к ZooKeeper: {e}")
        raise
# Линейный график для трендов транзакций по времени
def plot_trend_over_time(data):
    try:
        logger.info("Построение тренда транзакций по времени")
        if 'Date' in data.columns:
            data['Date'] = pd.to_datetime(data['Date'])  # Преобразуем колонку Date в формат datetime
            time_data = data.groupby('Date')['Amount'].sum()
            time_data.plot(kind='line', figsize=(10, 6), marker='o', color='orange')
            plt.title("Тренд транзакций по времени")
            plt.xlabel("Дата")
            plt.ylabel("Сумма транзакций")
            plt.tight_layout()
            plt.savefig("data/transaction_trend.png")
            plt.show()
            logger.info("График трендов сохранен как 'data/transaction_trend.png'")
        else:
            logger.warning("Колонка 'Date' отсутствует в данных, тренд по времени невозможно построить.")
    except Exception as e:
        logger.error(f"Ошибка при построении трендов: {e}")
        raise
# Тепловая карта распределения транзакций
def plot_heatmap(data):
    try:
        logger.info("Построение тепловой карты транзакций")
        if 'Category' in data.columns and 'Region' in data.columns:
            pivot_table = data.pivot_table(values='Amount', index='Region', columns='Category', aggfunc='sum', fill_value=0)
            sns.heatmap(pivot_table, annot=True, fmt='.0f', cmap='YlGnBu')
            plt.title("Тепловая карта транзакций (Сумма)")
            plt.xlabel("Категория")
            plt.ylabel("Регион")
            plt.tight_layout()
            plt.savefig("data/transaction_heatmap.png")
            plt.show()
            logger.info("Тепловая карта сохранена как 'data/transaction_heatmap.png'")
        else:
            logger.warning("Для тепловой карты нужны колонки 'Category' и 'Region', но они отсутствуют.")
    except Exception as e:
        logger.error(f"Ошибка при построении тепловой карты: {e}")
        raise

# Круговая диаграмма для суммы транзакций по регионам
def plot_pie_chart(region_data):
    try:
        logger.info("Построение круговой диаграммы для суммы транзакций по регионам")
        region_data.plot(kind='pie', autopct='%1.1f%%', startangle=140, figsize=(8, 8), legend=False)
        plt.title("Распределение суммы транзакций по регионам")
        plt.ylabel("")  # Убираем подпись оси Y
        plt.tight_layout()
        plt.savefig("data/region_pie_chart.png")
        plt.show()
        logger.info("Круговая диаграмма сохранена как 'data/region_pie_chart.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении круговой диаграммы: {e}")
        raise

# Функция для отображения суммы транзакций по регионам
def plot_region_summary(region_data):
    try:
        logger.info("Построение графика суммы транзакций по регионам")
        region_data.plot(kind='bar', color='skyblue', title="Сумма транзакций по регионам")
        plt.xlabel("Регион")
        plt.ylabel("Сумма транзакций")
        plt.tight_layout()
        plt.savefig("data/region_summary.png")
        plt.show()
        logger.info("График сохранен как 'data/region_summary.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении графика: {e}")
        raise


# График распределения суммы транзакций
def plot_transaction_distribution(data):
    try:
        logger.info("Построение графика распределения транзакций")
        sns.histplot(data['Amount'], kde=True, color='green')
        plt.title("Распределение суммы транзакций")
        plt.xlabel("Сумма транзакции")
        plt.ylabel("Частота")
        plt.tight_layout()
        plt.savefig("data/transaction_distribution.png")
        plt.show()
        logger.info("График сохранен как 'data/transaction_distribution.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении графика: {e}")
        raise


# График "ящик с усами" (boxplot) для суммы транзакций по регионам
def plot_boxplot_by_region(data):
    try:
        logger.info("Построение boxplot для транзакций по регионам")
        sns.boxplot(x='Region', y='Amount', data=data, palette='coolwarm')
        plt.title("Boxplot: Сумма транзакций по регионам")
        plt.xlabel("Регион")
        plt.ylabel("Сумма транзакций")
        plt.tight_layout()
        plt.savefig("data/boxplot_by_region.png")
        plt.show()
        logger.info("График сохранен как 'data/boxplot_by_region.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении графика: {e}")
        raise
# Тренд транзакций по регионам
def plot_time_trend_by_region(data):
    try:
        logger.info("Построение трендов транзакций по регионам")
        data['Date'] = pd.to_datetime(data['Date'])  # Убедимся, что формат даты правильный
        time_region_data = data.groupby(['Date', 'Region'])['Amount'].sum().unstack()
        time_region_data.plot(figsize=(12, 8), marker='o')
        plt.title("Тренды транзакций по регионам")
        plt.xlabel("Дата")
        plt.ylabel("Сумма транзакций")
        plt.legend(title="Регион")
        plt.tight_layout()
        plt.savefig("data/time_trend_by_region.png")
        plt.show()
        logger.info("График трендов транзакций по регионам сохранен как 'data/time_trend_by_region.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении трендов по регионам: {e}")
        raise
# Частота транзакций по типу продукта
def plot_transaction_frequency(data):
    try:
        logger.info("Построение диаграммы частоты транзакций по типу продукта")
        product_counts = data['Product Type'].value_counts()
        product_counts.plot(kind='bar', figsize=(10, 6), color='teal')
        plt.title("Частота транзакций по типу продукта")
        plt.xlabel("Тип продукта")
        plt.ylabel("Количество транзакций")
        plt.tight_layout()
        plt.savefig("data/transaction_frequency.png")
        plt.show()
        logger.info("Диаграмма частоты транзакций сохранена как 'data/transaction_frequency.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении диаграммы частоты транзакций: {e}")
        raise
# Матрица корреляции
def plot_correlation_matrix(data):
    try:
        logger.info("Построение матрицы корреляции")
        numeric_data = data.select_dtypes(include='number')  # Выбираем только числовые колонки
        correlation_matrix = numeric_data.corr()
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
        plt.title("Матрица корреляции")
        plt.tight_layout()
        plt.savefig("data/correlation_matrix.png")
        plt.show()
        logger.info("Матрица корреляции сохранена как 'data/correlation_matrix.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении матрицы корреляции: {e}")
        raise

# График суммы транзакций по типу продукта
def plot_product_summary(data):
    try:
        logger.info("Построение диаграммы суммы транзакций по типу продукта")
        product_data = data.groupby('Product Type')['Amount'].sum().sort_values(ascending=False)
        product_data.plot(kind='bar', figsize=(10, 6), color='purple')
        plt.title("Сумма транзакций по типу продукта")
        plt.xlabel("Тип продукта")
        plt.ylabel("Сумма транзакций")
        plt.tight_layout()
        plt.savefig("data/product_summary.png")
        plt.show()
        logger.info("График суммы транзакций по типу продукта сохранен как 'data/product_summary.png'")
    except Exception as e:
        logger.error(f"Ошибка при построении графика суммы транзакций по продуктам: {e}")
        raise

# Функция обработки данных
def process_data(data: pd.DataFrame):
    try:
        logger.info("Начало обработки данных")

        # Фильтрация транзакций с суммой больше 500
        filtered_data = data[data['Amount'] > 500]
        logger.info(f"Найдено {len(filtered_data)} записей с суммой транзакций > 500")

        # Группировка данных по регионам
        region_data = filtered_data.groupby('Region')['Amount'].sum()
        logger.info(f"Общая сумма транзакций по регионам: {region_data}")

        # Построение графиков
        plot_region_summary(region_data)
        plot_transaction_distribution(data)
        plot_boxplot_by_region(filtered_data)
        plot_pie_chart(region_data)
        plot_trend_over_time(data)
        plot_heatmap(data)
        plot_product_summary(data)
        plot_time_trend_by_region(data)
        plot_transaction_frequency(data)
        plot_correlation_matrix(data)

        # Сохранение отфильтрованных данных в новый CSV файл
        filtered_data.to_csv("data/processed_transaction_data.csv", index=False)
        logger.info("Отфильтрованные данные сохранены в 'data/processed_transaction_data.csv'")

    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        raise

