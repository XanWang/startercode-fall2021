from mrjob.job import MRJob

class LetterCount(MRJob):

    def mapper_init(self):
        pass

    def mapper(self, key, line):
        for symbol in line:
            if symbol.isalpha():
                yield (symbol, 1)

    def mapper_final(self):
        pass

    def reducer_init(self):
        pass

    def reducer(self, key, values):
        yield (key, sum(values))

    def reducer_final(self):
        pass

if __name__ == '__main__':
    LetterCount.run()
