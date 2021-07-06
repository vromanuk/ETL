import abc
import json
import logging
import os
from typing import Any, Optional

from redis import Redis


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass

    @abc.abstractmethod
    def clean_up(self):
        """Очистить состояние"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        if self.file_path is None:
            return

        with open(self.file_path, "w") as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        if self.file_path is None:
            logging.info("No state file provided. Continue with in-memory state")
            return {}

        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)

            return data

        except FileNotFoundError:
            self.save_state({})
            return {}

    def clean_up(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)


class State:
    """
     Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.retrieve_state()

    def retrieve_state(self) -> dict:
        data = self.storage.retrieve_state()
        if not data:
            return {}
        return data

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.state[key] = value

        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.state.get(key)

    def clean_up(self):
        self.storage.clean_up()


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: dict) -> None:
        self.redis_adapter.set("data", json.dumps(state))

    def retrieve_state(self) -> dict:
        raw_data = self.redis_adapter.get("data")
        if raw_data is None:
            return {}
        return json.loads(raw_data)

    def clean_up(self):
        self.redis_adapter.flushdb()
