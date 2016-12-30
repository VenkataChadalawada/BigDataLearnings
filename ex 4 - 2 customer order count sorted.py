from mrjob.job import MRJob
from mrjob.step import MRStep

class SpendByCustomerSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orders,
                   reducer=self.reducer_totals_by_customer),
            MRStep(mapper=self.mapper_make_amounts_key,
                   reducer=self.reducer_output_results)
        ]
    def mapper_get_orders(self, _, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer_totals_by_customer(self, customerID, orders):
        yield customerID, sum(orders)

    def mapper_make_amounts_key(self, customerID, orderTotal):
        yield '%04.02f'%float(orderTotal), customerID
        
    def reducer_output_results(self, orderTotal, customerIDs):
        for customerID in customerIDs:
            yield customerID, orderTotal

if __name__ == '__main__':
    SpendByCustomerSorted.run()
    



Out put of first reducer
"0"	5524.950000000002
"1"	4958.600000000001
"10"	4819.7
"11"	5152.29
"12"	4664.589999999999
"13"	4367.619999999999
"14"	4735.030000000001
"15"	5413.510000000001
"16"	4979.060000000001
"17"	5032.680000000001
"18"	4921.27
"19"	5059.429999999998
"2"	5994.590000000001
"20"	4836.86


Output:
"45"	"3309.38"
"79"	"3790.57"
"96"	"3924.23"
"23"	"4042.65"
"99"	"4172.29"
"75"	"4178.50"
"36"	"4278.05"
"98"	"4297.26"
