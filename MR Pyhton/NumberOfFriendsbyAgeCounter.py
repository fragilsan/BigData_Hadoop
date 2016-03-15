from mrjob.job import MRJob

class MRNumberOfFriendsByAgeCounter(MRJob):
    def mapper(self, _,line):
        (userID,Name,Age,NumberOfFriends) = line.split(',')
        yield Age, float(NumberOfFriends)
        
    def reducer(self,Age,NumberOfFriends):
        total = 0
        numElements = 0
        for x in NumberOfFriends:
            total+=x
            numElements+=1
        yield Age, total/numElements
        
if __name__ == '__main__':
    MRNumberOfFriendsByAgeCounter.run()