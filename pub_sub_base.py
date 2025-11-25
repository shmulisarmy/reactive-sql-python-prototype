from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterable
########
from typing import Callable

class Observable:
    def __init__(self):
        self.observers: list['Subscriber'] = []

    def subscribe(self, observer: 'Subscriber'):
        print(f'{self = }')
        
        self.observers.append(observer)
        observer.receiving_from = self

    def unsubscribe(self, observer: 'Subscriber'):
        self.observers.remove(observer)

    def publish_add(self, data):
        for observer in self.observers:
            observer.on_add(data)

    def publish_remove(self, data):
        for observer in self.observers:
            observer.on_remove(data)

    def publish_update(self, old_data, new_data):
        for observer in self.observers:
            observer.on_update(old_data, new_data)

    @abstractmethod
    def pull(self) -> Iterable:
        raise NotImplementedError

    ######
    def filter_on(self, predicate: Callable):
        from implementations import Filter
        f = Filter(predicate)
        self.subscribe(f)
        return f
    def map_on(self, transformer: Callable):
        from implementations import Filter
        m = Filter(transformer)
        self.subscribe(m)
        return m   
    def display_on(self):
        from implementations import Printer
        p = Printer()
        self.subscribe(p)
        p.pull()
        return p   


class Subscriber(
    ABC
):
    def __init__(self):
        self.receiving_from: Observable

    

    @abstractmethod
    def on_add(self, data):
        pass

    @abstractmethod
    def on_remove(self, data):
        pass


    @abstractmethod
    def on_update(self, old_data, new_data):
        pass