from mrjob.job import MRJob
class MRFriendsByAge(MRJob):
    def mapper(self,key,line):
        (ID,name,age,numFriends) = line.split(',')
        yield age,float(numFriends)
        
    def reducer(self,age,numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
            
        yield age, total / numElements
   
if __name__ == '__main__':
       MRFriendsByAge.run()
       
Sample Input:-------
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21
5,Weyoun,59,318

Sample output:--------
"18"	343.375
"19"	213.27272727272728
"20"	165.0
"21"	350.875
"22"	206.42857142857142
"23"	246.3
"24"	233.8
"25"	197.45454545454547
"26"	242.05882352941177
"27"	228.125
