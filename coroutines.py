import math
import random
from time import sleep

from postgres_to_es.utils import coroutine


def cash_return(deposit: int, percent: float, years: int) -> float:
    value = math.pow(1 + percent / 100, years)
    return round(deposit * value, 2)


def cash_return_coro(percent: float, years: int) -> float:
    value = math.pow(1 + percent / 100, years)
    while True:
        try:
            deposit = yield
            yield round(deposit * value, 2)
        except GeneratorExit:
            print("Выход из корутины")


def run_cash_return_coro():
    coro = cash_return_coro(5, 5)
    next(coro)
    values = [1000, 2000, 5000, 10000, 100000]
    for item in values:
        print(coro.send(item))
        next(coro)
    coro.close()


def double_it():
    while True:
        try:
            number = yield
            yield pow(number, 2)
        except GeneratorExit:
            print("Exit")


def run_double_it_coro():
    coro = double_it()
    next(coro)
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    for n in values:
        print(coro.send(n))
        next(coro)


def generate_numbers(target):
    while True:
        value = random.randint(1, 11)
        target.send(value)
        sleep(0.1)


@coroutine
def double_odd(target):
    while value := (yield):
        if value % 2 != 0:
            value = value ** 2
        target.send(value)


@coroutine
def halve_even(target):
    while value := (yield):
        if value % 2 == 0:
            value = value // 2
        target.send(value)


@coroutine
def print_sum():
    buf = []
    while value := (yield):
        buf.append(value)
        if len(buf) == 10:
            print(sum(buf))
            buf.clear()


if __name__ == "__main__":
    # run_cash_return_coro()
    # run_double_it_coro()
    printer_sink = print_sum()
    even_filter = halve_even(printer_sink)
    odd_filter = double_odd(even_filter)
    generate_numbers(odd_filter)
