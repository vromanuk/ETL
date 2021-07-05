import json
import logging
from dataclasses import asdict
from urllib.parse import urljoin

import backoff
import requests
from psycopg2 import OperationalError
from psycopg2.extensions import connection as _connection
from pydantic.dataclasses import dataclass

from etl_state import RedisStorage

logging.basicConfig(format="[%(asctime)s: %(levelname)s] %(message)s", level=logging.INFO)


@dataclass
class ESItem:
    id: str
    genres: list[str]
    writers: list[str]
    actors: list[str]
    imdb_rating: float
    title: str
    directors: list[str]
    description: str


class ESLoader:
    def __init__(self, url: str, storage: RedisStorage):
        self.url = url
        self.storage = storage

    @staticmethod
    def _get_es_bulk_query(rows: list[ESItem], index_name: str = "movies") -> list[str]:
        """
        Подготавливает bulk-запрос в Elasticsearch
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend(
                [json.dumps({"index": {"_index": index_name, "_id": row.id}}), json.dumps(asdict(row))]
            )
        return prepared_query

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_tries=3,
        jitter=backoff.random_jitter,
    )
    def load_to_es(self, records: list[ESItem], index_name: str = "movies"):
        """
        Отправка запроса в ES и разбор ошибок сохранения данных
        """
        data = self.storage.retrieve_state()
        prepared_query = data.get("prepared_query")
        if not prepared_query:
            prepared_query = self._get_es_bulk_query(records, index_name)
            self.storage.save_state({"prepared_query": prepared_query})
        str_query = "\n".join(prepared_query) + "\n"

        logging.info("loading movies to elastic")
        response = requests.post(
            urljoin(self.url, "_bulk"), data=str_query, headers={"Content-Type": "application/x-ndjson"}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response["items"]:
            error_message = item["index"].get("error")
            if error_message:
                logging.error(error_message)

        self.storage.clean_up()


class PostgresLoader:
    BATCH_LIMIT = 100

    def __init__(self, conn: _connection, storage: RedisStorage):
        self.conn = conn
        self.storage = storage

    @backoff.on_exception(backoff.expo, OperationalError, max_tries=3, jitter=backoff.random_jitter)
    def load_movies(self) -> dict:
        """
        Основной метод для ETL.
        """
        with self.conn.cursor() as cur:
            records = self.storage.retrieve_state()
            if records:
                return records
            logging.info("loading cast")
            cur.execute(
                f"""
                    SELECT "movies_person"."uuid"
                    FROM "movies_person"
                    WHERE "movies_person"."modified" < CURRENT_DATE
                    ORDER BY "movies_person"."modified"
                    LIMIT {self.BATCH_LIMIT};
                """
            )
            cast_ids = cur.fetchall()

            logging.info("loading film_work_ids")
            cur.execute(
                f"""
                SELECT fw.id
                    FROM movies_filmwork fw
                    LEFT JOIN movies_cast pfw ON pfw.film_work_id = fw.id
                    WHERE fw.modified < CURRENT_TIMESTAMP AND pfw.person_id::text = ANY(%(cast_ids)s)
                    ORDER BY fw.modified
                    LIMIT {self.BATCH_LIMIT};
                """,
                {"cast_ids": cast_ids or []},
            )
            film_work_ids = cur.fetchall()

            logging.info("loading film_works")
            cur.execute(
                """
                SELECT
                    "movies_filmwork"."title",
                    "movies_filmwork"."description",
                    "movies_filmwork"."rating",
                    "movies_filmwork"."uuid" AS "id",
                ARRAY_AGG("movies_genre"."genre" ) AS "genres",
                ARRAY_AGG(CONCAT("movies_person"."first_name", ' ', "movies_person"."last_name") )
                FILTER (WHERE "movies_role"."role" = 'actor') AS "actors",
                ARRAY_AGG(CONCAT("movies_person"."first_name", ' ', "movies_person"."last_name") )
                FILTER (WHERE "movies_role"."role" = 'director') AS "directors",
                ARRAY_AGG(CONCAT("movies_person"."first_name", ' ', "movies_person"."last_name") )
                FILTER (WHERE "movies_role"."role" = 'writer') AS "writers"
                FROM "movies_filmwork"
                LEFT OUTER JOIN "movies_filmwork_genres"
                    ON ("movies_filmwork"."id" = "movies_filmwork_genres"."filmwork_id")
                LEFT OUTER JOIN "movies_genre"
                    ON ("movies_filmwork_genres"."genre_id" = "movies_genre"."id")
                LEFT OUTER JOIN "movies_cast"
                    ON ("movies_filmwork"."id" = "movies_cast"."film_work_id")
                LEFT OUTER JOIN "movies_person"
                    ON ("movies_cast"."person_id" = "movies_person"."uuid")
                LEFT OUTER JOIN "movies_role" ON ("movies_cast"."role_id" = "movies_role"."id")
                WHERE "movies_filmwork"."id" = ANY(%(film_work_ids)s)
                GROUP BY "movies_filmwork"."title", "movies_filmwork"."description", "movies_filmwork"."creation_date",
                "movies_filmwork"."rating", "movies_filmwork"."type", "movies_filmwork"."uuid"
                ORDER BY "movies_filmwork"."rating" DESC;
            """,
                {"film_work_ids": film_work_ids or []},
            )
            records = cur.fetchall()

        self.storage.save_state(records)
        return records

    @staticmethod
    def transform_data(raw_data: dict) -> list[ESItem]:
        logging.info("transforming film_works to load into elastic")
        records = []
        for film_work in raw_data:
            title, description, imdb_rating, uuid, genres, actors, directors, writers = film_work
            es_item = ESItem(
                id=uuid,
                title=title,
                description=description,
                imdb_rating=imdb_rating,
                actors=actors or [],
                writers=writers or [],
                directors=directors or [],
                genres=genres or [],
            )
            records.append(es_item)

        return records