from mrjob.job import MRJob
class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore") #avoids issues in mrjob 5.0
            yield word.lower(), 1
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    MRWordFrequencyCount.run()


Sample Input:--------------
Freedom to Live Where You Want
Freedom to Work When You Want
Freedom to Work How You Want
Is Self-Employment for You? 
Flowchart: Should I Even Consider Self-Employment? 
Having a Financial Safety Net
Planning for Health Care
Self-Assessment Quiz
PART II: Making it Happen

Sample output:-------------
"abstract"	1
"accelerator"	1
"accept"	8
"accept."	1
"accepted"	3
"accepting"	1
"access"	4
"accident"	3
"accompanied"	3
"accompanies"	1
