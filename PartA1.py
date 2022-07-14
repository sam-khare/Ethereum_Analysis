from mrjob.job import MRJob
import time

class PartA1(MRJob):
        def mapper(self, _,line):
                try:
                        
                        fields=line.split(',')
                        if len(fields) == 7:
                                timestmp = int(fields[6])
                                month_year = time.strftime("%m-%y",time.gmtime(timestmp))
                                yield(month_year,1)
                except:
                        pass

        def combiner(self,feature,counts):
                yield(feature,sum(counts))
        def reducer(self,feature,counts):
                yield(feature,sum(counts))

if __name__ == '__main__':
        PartA1.run()
