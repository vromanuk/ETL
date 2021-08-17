import datetime
from uuid import UUID

from pydantic.dataclasses import dataclass


@dataclass
class ESItem:
    id: str
    genres: list[str]
    writers: list[str]
    actors: list[str]
    imdb_rating: float
    modified: str
    title: str
    directors: list[str]
    description: str


@dataclass
class ESGenreItem:
    id: int
    modified: datetime.datetime
    created: datetime.datetime
    genre: str


@dataclass
class ESPersonItem:
    uuid: UUID
    modified: datetime.datetime
    created: datetime.datetime
    first_name: str
    last_name: str
    birth_date: datetime.date
