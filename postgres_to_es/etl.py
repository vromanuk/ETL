import json
import logging
from dataclasses import asdict
from datetime import datetime
from urllib.parse import urljoin

import backoff
import requests
from psycopg2 import OperationalError
from psycopg2.extensions import connection as _connection

from etl_state import State
from postgres_to_es.entities import ESItem
from postgres_to_es.utils import coroutine

logging.basicConfig(format="[%(asctime)s: %(levelname)s] %(message)s", level=logging.INFO)


class ESSaver:
    def __init__(self, url: str, state: State):
        self.url = url
        self.state = state

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
    @coroutine
    def load_to_es(self, index_name: str = "movies"):
        """
        Отправка запроса в ES и разбор ошибок сохранения данных
        """
        try:
            while records := (yield):
                prepared_query = self._get_es_bulk_query(records, index_name)
                self.state.set_state("prepared_query", prepared_query)
                str_query = "\n".join(prepared_query) + "\n"

                logging.info("loading movies to elastic")
                response = requests.post(
                    urljoin(self.url, "_bulk"), data=str_query, headers={"Content-Type": "application/x-ndjson"}
                )
                self.state.set_state("modified", records[0].modified)

                json_response = json.loads(response.content.decode())
                for item in json_response["items"]:
                    error_message = item["index"].get("error")
                    if error_message:
                        logging.error(error_message)
        except GeneratorExit:
            self.state.clean_up()


class PostgresLoader:
    BATCH_LIMIT = 100

    def __init__(self, conn: _connection, state: State):
        self.conn = conn
        self.state = state

    @backoff.on_exception(backoff.expo, OperationalError, max_tries=3, jitter=backoff.random_jitter)
    def load_movies(self, coro):
        """
        Основной метод для ETL.
        """
        with self.conn.cursor() as cur:
            modified = self.state.get_state("modified")
            if not modified:
                logging.info("fetching min modified field")
                cur.execute(
                    """
                        SELECT MIN("movies_person"."modified") AS min_modified
                        FROM "movies_person"
                    """
                )
                modified = cur.fetchone()["min_modified"]
            else:
                modified = datetime.fromisoformat(modified)
            logging.info("loading cast")
            cur.execute(
                """
                    SELECT "movies_person"."uuid"
                    FROM "movies_person"
                    WHERE "movies_person"."modified" >= %(modified)s
                    ORDER BY "movies_person"."modified";
                """,
                {"modified": modified},
            )
            raw_cast_ids = cur.fetchall()
            cast_ids = [uuid["uuid"] for uuid in raw_cast_ids]

            logging.info("loading film_work_ids")
            cur.execute(
                """
                SELECT DISTINCT fw.id, fw.modified
                    FROM movies_filmwork fw
                    LEFT JOIN movies_cast pfw ON pfw.film_work_id = fw.id
                    WHERE fw.modified >= %(modified)s OR pfw.person_id::text = ANY(%(cast_ids)s)
                    ORDER BY fw.modified;
                """,
                {"cast_ids": cast_ids or [], "modified": modified},
            )
            raw_film_work_ids = cur.fetchall()
            film_work_ids = [id_["id"] for id_ in raw_film_work_ids]

            logging.info("loading film_works")
            cur.execute(
                """
                SELECT
                    "movies_filmwork"."title",
                    "movies_filmwork"."description",
                    "movies_filmwork"."rating",
                    "movies_filmwork"."modified",
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
                "movies_filmwork"."rating", "movies_filmwork"."modified", "movies_filmwork"."uuid"
                ORDER BY "movies_filmwork"."modified" DESC;
            """,
                {"film_work_ids": film_work_ids or []},
            )
            while rows := cur.fetchmany(self.BATCH_LIMIT):
                if not rows:
                    coro.close()
                coro.send(rows)

    @staticmethod
    @coroutine
    def transform_data(coro):
        logging.info("transforming film_works to load into elastic")
        try:
            while raw_data := (yield):
                records = []
                for film_work in raw_data:
                    es_item = ESItem(
                        id=film_work["id"],
                        title=film_work["title"],
                        description=film_work["description"],
                        imdb_rating=film_work["rating"],
                        modified=film_work["modified"].isoformat(),
                        actors=film_work["actors"] or [],
                        writers=film_work["writers"] or [],
                        directors=film_work["directors"] or [],
                        genres=film_work["genres"] or [],
                    )
                    records.append(es_item)
                coro.send(records)
        except GeneratorExit:
            coro.close()
