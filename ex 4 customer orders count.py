from mrjob.job import MRJob

class CustomerOrdersCount(MRJob):
    def mapper(self, key, line):
        (custID,item,orderAmount) = line.split(',')
        yield custID, float(orderAmount)
        
    def reducer(self, custID, amounts):
        yield custID, sum(amounts)
        
if __name__ == '__main__':
    CustomerOrdersCount.run()


How to run :
!python CustomerOrderCount.py customer-orders.csv > customercountout.txt
