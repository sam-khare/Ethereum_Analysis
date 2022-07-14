from mrjob.job import MRJob
import time

class PartA2(MRJob):
       
       def mapper(self, _,line):
            try:
                  fields = line.split(',')
                  if len(fields) == 7:
                          timestmp = int(fields[6])
                          tranc_value = float(fields[3])/1000000000000000000 #converting wei to ether
                          month_year = time.strftime("%m-%y",time.gmtime(timestmp))
                          yield(month_year,(tranc_value,1))
            except:
                  pass


        
       def combiner(self,feature,values):
            total_count = 0                        
            total_value = 0.0
            for value in values:
                    total_count += value[1]
                    total_value += value[0] 
            yield(feature,(total_value,total_count))

       def reducer(self,feature,values):
            total_count = 0
            total_value = 0.0
            for value in values:
                    total_count += value[1]
                    total_value += value[0]
            yield(feature,(total_value/total_count))


if __name__ == '__main__':
       PartA2.run()
