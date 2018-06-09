import random


def inside():
    x, y = random.random(), random.random()
    return x*x + y*y < 1
