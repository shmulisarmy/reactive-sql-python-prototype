from typing import Callable

from database_pub_sub_implementations import Table
from implementations import Collection, Filter, LiveJoin, Mapper, Printer
from utils import dict_add

people = Collection(
    [
        {"id": 1, "name": "John", "age": 18},
        {"id": 2, "name": "Jane", "age": 29},
        {"id": 3, "name": "Bob", "age": 16},
    ],
)



def bumper(row: dict)->dict:
    return {"id": row['id'], "name": row['name'], "age": row['age']+1}
transformer = Mapper(bumper)
people.subscribe(transformer)
transformer.subscribe(Printer())


people.add({"id": 4, "name": "mendel the humble", "age": 30},)