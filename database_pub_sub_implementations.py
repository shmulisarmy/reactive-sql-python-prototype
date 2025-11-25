from collections import defaultdict

from typing_extensions import Dict

from implementations import *
from pub_sub_base import *


@dataclass
class Channel(Observable):
    # the part of a table that is indexed, when you do ... where age = 18, this is what manages all rows where age = 18
    def __init__(self, table: "Table"):
        super().__init__()
        self.table = table
        self.row_ids: list[int] = []

    def when_row_hits_index(self, row_id: int):
        self.row_ids.append(row_id)
        self.publish_add(self.table.data[row_id])

    def when_row_misses_index(self, row_id: int):
        self.row_ids.remove(row_id)
        self.publish_remove(self.table.data[row_id])

    def when_row_updates_index(self, old_row_id: int, new_row_id: int):
        self.row_ids.remove(old_row_id)
        self.row_ids.append(new_row_id)
        self.publish_remove(self.table.data[old_row_id])
        self.publish_add(self.table.data[new_row_id])

    def pull(self) -> Iterable[dict]:
        for row_id in self.row_ids:
            yield self.table.data[row_id]


@dataclass
class Index:
    def __init__(self, col_indexing_on: str, table: "Table"):
        self.col_indexing_on = col_indexing_on
        self.table = table
        self.channels_by_indexed_col_value: defaultdict[any, Channel] = defaultdict(
            lambda: Channel(self.table)
        )
        for row in table.data:
            self.when_table_adds(row)

    def when_table_adds(self, data: dict):
        col_value = data[self.col_indexing_on]
        self.channels_by_indexed_col_value[col_value].when_row_hits_index(
            self.table.data.index(data)
        )

    def when_table_removes(self, data: dict):
        col_value = data[self.col_indexing_on]
        self.channels_by_indexed_col_value[col_value].when_row_misses_index(
            self.table.data.index(data)
        )

    def when_table_updates(self, old_data: dict, new_data: dict):
        old_col_value = old_data[self.col_indexing_on]
        new_col_value = new_data[self.col_indexing_on]
        self.channels_by_indexed_col_value[old_col_value].when_row_updates_index(
            self.table.data.index(old_data), self.table.data.index(new_data)
        )

    # def listen_to_channel(self, col_value: any, subscriber: Subscriber):
    #     if not self.channels_by_indexed_col_value.get(col_value):
    #         self.channels_by_indexed_col_value[col_value] = Channel(self.table)
    #     self.channels_by_indexed_col_value[col_value].subscribe(subscriber)

    def pull(self) -> Iterable[dict]:
        for col_value, channel in self.channels_by_indexed_col_value.items():
            for row_id in channel.row_ids:
                yield self.table.data[row_id]


class Table(Collection):
    def __init__(self, name: str, columns: list[str], data: list[dict]):
        self.name = name
        self.columns = columns
        self.indexes: list["Index"] = []
        super().__init__(data)

    def index_on(self, col_name: str):
        for index in self.indexes:
            if index.col_indexing_on == col_name:
                return index
        index = Index(col_name, self)
        self.indexes.append(index)
        return index

    def table_add(self, data):
        self.add(data)  # inherited from Collection
        for index in self.indexes:
            index.when_table_adds(data)

    def table_remove(self, data):
        self.remove(data)  # inherited from Collection
        for index in self.indexes:
            index.when_table_removes(data)

    def table_update(self, old_data, new_data):
        self.update(old_data, new_data)  # inherited from Collection
        for index in self.indexes:
            index.when_table_updates(old_data, new_data)

    def pull(self) -> Iterable:
        return self.data

    ######
    #
    def verify_row(self, new_row: dict):
        for col in new_row:
            if col not in self.columns:
                raise ValueError(
                    f"Column '{col}' does not exist in table '{self.name}'"
                )
        for col in self.columns:
            if col == "id":
                continue  ##the id gets auto-generated
            elif col not in new_row:
                raise ValueError(
                    f"Column '{col}' does not exist in table '{self.name}'"
                )

    def insert(self, new_row):
        self.verify_row(new_row)
        print(f'{len(self.data) = }')
        new_row["id"] = len(self.data)
        self.table_add(new_row)
        print(f"inserted {new_row}")
        
