from typing import Iterable
from dataclasses import dataclass, field
from pub_sub_base import Observable, Subscriber
from typing import Callable

from utils import dict_add


class Collection(Observable):
    def __init__(self, data: list | None = None):
        self.data: list[dict] = data or []
        super().__init__()

    def add(self, data):
        self.data.append(data)
        self.publish_add(data)

    def remove(self, data):
        self.data.remove(data)
        self.publish_remove(data)

    def update(self, old_data, new_data):
        self.data[self.data.index(old_data)] = new_data
        self.publish_update(old_data, new_data)

    def pull(self) -> Iterable[dict]:
        for row in self.data:
            yield row



@dataclass
class Filter(Observable, Subscriber):

    def __init__(self, predicate: Callable):
        super().__init__()
        self.predicate: Callable = predicate

    def on_add(self, data):
        if self.predicate(data):
            self.publish_add(data)

    def on_remove(self, data):
        if self.predicate(data):
            self.publish_remove(data)

    def on_update(self, old_data, new_data):
        if self.predicate(old_data):
            self.publish_remove(old_data)
        if self.predicate(new_data):
            self.publish_add(new_data)

    def pull(self) -> Iterable[dict]:
        for row in self.receiving_from.pull():
            if self.predicate(row):
                yield row




class Mapper(Observable, Subscriber):

    def __init__(self, transformer: Callable):
        super().__init__()
        self.transformer: Callable = transformer

    def add(self, data):
        self.publish_add(self.transformer(data))

    def remove(self, data):
        self.publish_remove(self.transformer(data))

    def update(self, old_data, new_data):
        self.publish_update(self.transformer(old_data), self.transformer(new_data))

    def pull(self) -> Iterable[dict]:
        for row in self.receiving_from.pull():
            yield self.transformer(row)





@dataclass
class CustomSubscriber(Subscriber):
    receiving_from: Observable
    on_add_callback: Callable
    on_remove_callback: Callable
    on_update_callback: Callable
    pull_callback: Callable[[Observable], Iterable]|None

    def __init__(self, on_add_callback: Callable, on_remove_callback: Callable, on_update_callback: Callable, pull_callback: Callable[[Observable], Iterable]|None):
        self.receiving_from = None
        self.on_add_callback = on_add_callback
        self.on_remove_callback = on_remove_callback
        self.on_update_callback = on_update_callback
        self.pull_callback = pull_callback

    def on_add(self, data):
        self.on_add_callback(data)

    def on_remove(self, data):
        self.on_remove_callback(data)

    def on_update(self, old_data, new_data):
        self.on_update_callback(old_data, new_data)

    def pull(self) -> Iterable[dict]:
        if not self.pull_callback:
            raise NotImplementedError
        yield from self.pull_callback(self.receiving_from)

    ##########




class LiveJoin(Observable):

    def __init__(self, receiver1: Observable, receiver2: Observable):
        self.receiver1 = receiver1
        self.receiver2 = receiver2
        self.receiver1.subscribe(CustomSubscriber( lambda data: self.add_from_1(data), lambda data: self.remove_from_1(data), lambda old_data, new_data: self.update_from_1(old_data, new_data), None))
        self.receiver2.subscribe(CustomSubscriber(lambda data: self.add_from_2(data), lambda data: self.remove_from_2(data), lambda old_data, new_data: self.update_from_2(old_data, new_data), None))
        self.collected_from_1 = list(receiver1.pull())
        self.collected_from_2 = list(receiver2.pull())  
        super().__init__()

    def add_from_1(self, data):
        self.collected_from_1.append(data)
        for row2 in self.collected_from_2:
            self.publish_add(dict_add(data, row2))

    def remove_from_1(self, data):
        self.collected_from_1.remove(data)
        for row2 in self.collected_from_2:
            self.publish_remove(dict_add(data, row2))

    def update_from_1(self, old_data, new_data):
        self.collected_from_1[self.collected_from_1.index(old_data)] = new_data
        for row2 in self.collected_from_2:
            self.publish_remove(dict_add(old_data, row2))
            self.publish_add(dict_add(new_data, row2))

    ##############
    def add_from_2(self, data):
        self.collected_from_2.append(data)
        for row1 in self.collected_from_1:
            self.publish_add(dict_add(row1, data)) 

    def remove_from_2(self, data):
        self.collected_from_2.remove(data)
        for row1 in self.collected_from_1:
            self.publish_remove(dict_add(row1, data))

    def update_from_2(self, old_data, new_data):
        self.collected_from_2[self.collected_from_2.index(old_data)] = new_data
        for row1 in self.collected_from_1:
            old_joint_row = dict_add(row1, old_data)
            new_joint_row = dict_add(row1, new_data)
            self.publish_remove(old_joint_row)
            self.publish_add(new_joint_row)

    ##############

    def pull(self) -> Iterable[dict]:
        for row in self.collected_from_2:
            for row1 in self.collected_from_1:
                yield dict_add(row1, row)






class Printer(Subscriber):
    def on_add(self, data):
        print(f"Added: {data}")

    def on_remove(self, data):
        print(f"Removed: {data}")

    def on_update(self, old_data, new_data):
        print(f"Updated: {old_data} -> {new_data}")

    def pull(self):
        for row in self.receiving_from.pull():
            self.on_add(row)