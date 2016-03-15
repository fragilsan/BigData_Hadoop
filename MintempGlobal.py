from mrjob.job import MRJob

class MRMinTemperature(MRJob):
    def mapper(self,_,line):
        (station,date,data_type,data,x,y,z,w) = line.split(',')
        if (data_type=='TMIN'):
            yield station,float(data)/10
                
    def reducer(self, station,minTemperatures):
        minTemp=1000
        for x in minTemperatures:
            if(x<minTemp):
                    minTemp=x
        yield station,minTemp
        
if __name__ == '__main__':
    MRMinTemperature.run()