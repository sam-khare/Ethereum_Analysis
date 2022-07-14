from mrjob.job import MRJob
from mrjob.step import MRStep

class PartB(MRJob):
        def mapper_join(self, _,line):
                fields = line.split(',')
                try:
                         if (len(fields)== 7):
                                to_address = fields[2]
                                value = float(fields[3])
                                yield(to_address,(1,value))

                         elif (len(fields)==5):
                                cont_address = fields[0]
                                yield(cont_address,(2,1))                       
                  
                except:
                         pass

        def reducer_join(self,address,values):
                 flag = False
                 total_value = 0
                 for value in values:
                         if value[0] == 1:
                                 total_value += int(value[1])
                        
                         elif value[0] == 2:
                                 flag = True
                 
                 if flag:
                         yield(address,total_value)

        def mapper_sort(self,address, value):
                 yield None , (address, value)

        
        def reducer_sort(self, _,value):
                 sorted_values = sorted(value, reverse = True,key = lambda tup :tup[1])
                 i = 0
                 for val in sorted_values: 
                         yield(val[0],val[1])
                         i+=1
                         if i >= 10:
                              break


        def steps(self):
                 return [MRStep(mapper = self.mapper_join, reducer=self.reducer_join,jobconf={'mapreduce.job.reduces': '10'}), MRStep(mapper = self.mapper_sort, reducer = self.reducer_sort,jobconf={'mapreduce.job.reduces': '40'})]


if  __name__ == '__main__':
              PartB.run() 
                               
