MyWay:
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie (MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.mapper1, combiner = self.combiner1, reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]
        
    def mapper1(self, key, line):
        (userID,movieID,rating,timestamp) = line.split(',')
        yield movieID,1
        
    def combiner1(self, movieID, counts):
        yield movieID, sum(counts)
        
    def reducer1(self,movieID,counts):
        yield None, (sum(counts), movieID)
        
    def reducer2(self, key, values):
        yield max(values)
        
if __name__ == '__main__':
    MostPopularMovie.run()

Run:
!python TopMovieRating.py ml-20m/ratings.csv > topmovieout.txt

input:
userid,movieid,rating,timestamp

output:
67310	"296"

FrankWay:
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
