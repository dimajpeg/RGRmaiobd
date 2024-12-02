import sys
import os
import pandas as pd
from loguru import logger

# Добавляем путь к директории src в sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Попытка импорта после добавления пути
from curator_utils import process_data, connect_to_zookeeper

# Проверка и создание папки logs, если она не существует
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)  # Если папка уже существует, не будет вызвана ошибка

# Настройка логирования
logger.add(os.path.join(log_dir, "app.log"), format="{time} {level} {message}", level="INFO")

def main():
    try:
        logger.info("Запуск приложения")

        # Чтение данных из CSV файла
        data = pd.read_csv("data/transaction_data.csv")
        logger.info(f"Данные успешно загружены: {len(data)} записей")

        # Обработка данных
        process_data(data)

        # Подключение к ZooKeeper
        connect_to_zookeeper()

    except Exception as e:
        logger.error(f"Ошибка в приложении: {e}")

if __name__ == "__main__":
    main()

