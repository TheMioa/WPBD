from mrjob.job import MRJob
class MRWordCount(MRJob):

    def mapper(self, _, line):
        yield (line.split(',')[2], int(line.split('$')[1].split(',')[0].split('.')[0]))

    def reducer(self, key, values):
        s = 0 #sum of primes
        c = 0 #number of primes
        for p in values:
            s += p
            c += 1
        yield (key, s / c)

if __name__ == '__main__':
    MRWordCount.run()