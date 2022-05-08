from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w]+")

class MRWordCount(MRJob):
    
    def steps(self):
        return(
            MRStep(mapper=self.mapper_get_word,
            reducer=self.reducer_word_count),
            MRStep(reducer=self.reducer_find_max)
        )
    
    def mapper_get_word(self, _, line):
        yield (int(line.split('$')[1].split(',')[0].split('.')[0]),None)

    def reducer_word_count(self, word, counts):
        yield None, (word, None)

    def reducer_find_max(self, _, values):
        self.alist = []
        for value in values:
            self.alist.append(value)
        self.blist = []
        for i in range(5):
          self.blist.append(max(self.alist))
          self.alist.remove(max(self.alist))
        for i in range(5):
          yield self.blist[i]

if __name__ == '__main__':
    MRWordCount.run()