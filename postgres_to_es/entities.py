from pydantic.dataclasses import dataclass


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
