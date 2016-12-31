from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopNiceMovie(MRJob):
    
    def configure_options(self):
        super(MostPopNiceMovie,self).configure_options()
        self.add_file_option('--items',help='path to movie names')
        #reducer_init runs before reducer and that loads up, here we are using to load up a dictionary
    def steps(self):
        return[
         MRStep(mapper=self.mapper1,reducer_init=self.reducer_init,reducer=self.reducer1),
         MRStep(mapper=self.mapper_passthrough,reducer=self.reducer2)
        ]
        
    def mapper1(self,key,line):
        (userID,movieID,rating,timestamp) = line.split(',')
        yield movieID,1
        
    def reducer_init(self,movieID,count):
        self.movienames = {}
        with open("movies.csv") as f:
            for line in f:
                fields = line.split('|')
                self.movienames[fields[0]]=fields[1].decode('utf-8','ignore')
            
    def reducer1(self, movieID, counts):
        yield None, (sum(counts),self.movienames[key])
    
    def mapper_passthrough(self, key, value):
        yield key, value
            
    def reducer2(self, key, values):
        yield max(values)
        
if __name__ == '__main__':
    MostPopNiceMovie.run()

TO RUN:
    need to pass --items locaton and then input file
    !python TopMovieRatingAncillary.py --items=ml-20m/movies.csv ml-20m/ratings.csv > Anstop.txt
    
