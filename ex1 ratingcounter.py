from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer(self, rating, occurences):
        yield rating, sum(occurences)

if __name__ == '__main__':
    MRRatingCounter.run()
    
sample Input :
USERID  MOVIEID RATING  TIMESTAMP
196     242     3       686986986
186     302     3       897869896
196     768     1       879879879
234     73      3       298398738
166     879     7       897979790
186     781     2       872892020
186     234     3       876788989

sample output:
"1"     1
"2"     1
"3"     4
"7"     1
