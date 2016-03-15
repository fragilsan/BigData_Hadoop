from mrjob.job import MRJob
from mrjob.step import MRStep


class MRAmountSpentByCustomer(MRJob):
    
    def steps(self):
        return[MRStep(mapper=self.mapper_amount_by_customer,reducer=self.reducer_amount_by_customer),MRStep(mapper=self.mapper_sort_by_amount,reducer = self.reducer_sort_by_amount)]
        
        
    def mapper_amount_by_customer(self,_,line):
        (clienteID,pedidoID,gastado) = line.split(',')
        yield clienteID, float(gastado)
        
    def reducer_amount_by_customer(self,clienteID,gastado):
        yield clienteID, sum(gastado)    
        
        
        
    def mapper_sort_by_amount(self,userID,Amount):
        yield '%04.02f'%float(Amount),userID
        
    def reducer_sort_by_amount(self,Amount,userID):
        for x in userID:
            yield Amount,x
            
if __name__ == '__main__':
    MRAmountSpentByCustomer.run()