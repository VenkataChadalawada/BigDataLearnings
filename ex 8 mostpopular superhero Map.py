from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperHero(MRJob):

    def configure_options(self):
        super(MostPopularSuperHero, self).configure_options()
        self.add_file_option('--names', help='Path to Marvel-names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer=self.reducer_combine_friends),
            MRStep(mapper=self.mapper_prep_for_sort,
                   mapper_init=self.load_name_dictionary,
                   reducer = self.reducer_find_max_friends)
        ]

    def mapper_count_friends_per_line(self, _, line):
        fields = line.split()
        heroID = fields[0]
        numFriends = len(fields) - 1
        yield int(heroID), int(numFriends)

    def reducer_combine_friends(self, heroID, friendCounts):
        yield heroID, sum(friendCounts)

    def mapper_prep_for_sort(self, heroID, friendCounts):
        heroName = self.heroNames[heroID]
        yield None, (friendCounts, heroName)

    def reducer_find_max_friends(self, key, value):
        yield max(value)

    def load_name_dictionary(self):
        self.heroNames = {}

        with open("Marvel-names.txt") as f:
            for line in f:
                fields = line.split('"')
                heroID = int(fields[0])
                self.heroNames[heroID] = unicode(fields[1], errors='ignore')

if __name__ == '__main__':
    MostPopularSuperHero.run()
    
Inputs:---------
1.Marvel-Graph.txt
5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456 
5989 4080 4264 4446 3779 2430 2297 6169 3530 3272 4282 6432 2548 4140 185 105 3878 2429 1334 4595 2767 3956 3877 4776 4946 3407 128 269 5775 5121 481 5516 4758 4053 1044 1602 3889 1535 6038 533 3986 
5982 217 595 1194 3308 2940 1815 794 1503 5197 859 5096 6039 2664 651 2244 528 284 1449 1097 1172 1092 108 3405 5204 387 4607 4545 3705 4930 1805 4712 4404 247 4754 4427 1845 536 5795 5978 533 3984 6056 
5983 1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485 
5980 2731 3712 1587 6084 2472 2546 6313 875 859 323 2664 1469 522 2506 2919 2423 3624 5736 5046 1787 5776 3245 3840 2399 
5981 3569 5353 4087 2653 2058 2218 5354 5306 3135 4088 4869 2958 2959 5732 4076 4155 291 
5986 2658 3712 2650 1265 133 4024 6313 3120 6066 3546 403 545 4860 4337 2295 5467 128 2399 5999 5516 5309 4731 2557 5013 4132 5306 5615 2397 945 533 5694 824 1383 3771 592 5017 704 3778 1127 1480 274 5768 6148 4204 5250 4804 1715 2069 2548 525 2664 520 522 4978 6306 1259 5002 449 2449 1231 3662 3950 2603 2931 3319 3955 3210 5776 5088 2273 5576 1649 518 1535 3356 5874 5973 1660 4359 4188 2614 2613 3594 3805 3750 331 3757 1347 4366 66 2199 3296 3008 1425 3454 1638 1587 731 183 2 2689 505 5021 2629 5834 4441 2184 4607 4603 5716 969 867 6196 604 2438 155 2430 3632 5446 5696 4454 3233 6227 1116 1177 563 2728 5736 4898 859 5535 5046 2971 1805 1602 1289 3220 4589 3989 5931 3986 1369 

2.Marvel-Names.txt
1 "24-HOUR MAN/EMMANUEL"
2 "3-D MAN/CHARLES CHAN"
3 "4-D MAN/MERCURIO"
4 "8-BALL/"

HOW TO Run:--------
!python MostPopularSuperhero.py --names=Marvel-names.txt Marvel-Graph.txt > marvelout.txt

Output:--------
1933	"CAPTAIN AMERICA"

