from typing import Callable

from database_pub_sub_implementations import Table
from implementations import Collection, Filter, LiveJoin, Printer
from utils import dict_add

people = Table(
    "people",
    ["id", "name", "age"],
    [
        {"id": 1, "name": "John", "age": 18},
        {"id": 2, "name": "Jane", "age": 29},
        {"id": 3, "name": "Bob", "age": 16},
    ],
)


todos = Table(
    "todos",
    ["id", "title", "completed", "userId"],
    [
        {"id": 1, "title": "clean the room", "completed": False, "userId": 1},
        {"id": 2, "title": "mop the floor", "completed": False, "userId": 1},
        {"id": 3, "title": "wash the dishes", "completed": True, "userId": 2},
        {"id": 4, "title": "cook the meal", "completed": False, "userId": 3},
    ],
)


# todos.index_on("userId").channels_by_indexed_col_value[1].filter_on(
#     lambda todo: todo["completed"] == False
# ).display_on()
todos.table_add({"id": 5, "title": "cook the meal", "completed": False, "userId": 1})

people.filter_on(lambda person: person["age"] > 18).display_on()

todos_by_user = todos.index_on("userId")

my_todos = LiveJoin(
    todos_by_user.channels_by_indexed_col_value[1],
    people.index_on("id").channels_by_indexed_col_value[1],
)
printer = my_todos.display_on()
