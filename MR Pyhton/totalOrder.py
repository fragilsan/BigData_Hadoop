from mrjob.job import MRJob


class MRTotalWasted(MRJob):
    def mapper(self,_,line):
        (clientID,orderID,totalOrder) = line.split(',')
        yield clientID,float(totalOrder)
        
    def reducer(self,clientID,totals):
        tot = 0
        for x in totals:
            tot = tot + x
        yield clientID, tot
        
if __name__ == '__main__':
   MRTotalWasted.run()
        