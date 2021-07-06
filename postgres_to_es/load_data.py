import os

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from redis import Redis

from etl_state import RedisStorage, State
from postgres_to_es.etl import ESLoader, PostgresLoader

BASE_ES_URL = os.getenv("ELASTIC_URL")
redis = Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"))


def pipeline(pg_conn: _connection, es_url: str, redis_client: Redis):
    """Основной метод загрузки данных из Postgres в Elasticsearch"""

    redis_client = RedisStorage(redis_client)
    state = State(storage=redis_client)
    elastic_loader = ESLoader(es_url, state=state)
    postgres_loader = PostgresLoader(pg_conn, state=state)

    elastic_coro = elastic_loader.load_to_es()
    elastic_prepared_data = postgres_loader.transform_data(elastic_coro)
    postgres_loader.load_movies(elastic_prepared_data)


if __name__ == "__main__":
    dsl = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        pipeline(pg_conn, BASE_ES_URL, redis_client=redis)
