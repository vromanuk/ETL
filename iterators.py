class CyclicIterator:
    def __init__(self, cycles):
        self.current = -1
        self.start, self.step, self.stop = cycles

    def __iter__(self):
        return self

    def __next__(self):
        if self.current < self.stop:
            self.current += 1
            return self.current
        else:
            self.current = 0
            return self.current


def main():
    cyclic_iterator = CyclicIterator(range(3))
    for i in cyclic_iterator:
        print(i)


if __name__ == '__main__':
    main()
