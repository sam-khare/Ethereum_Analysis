from mrjob.job import MRJob
import time

class fork_chain(MRJob):
	def mapper(self,_,line):
		try:
			fields=line.split(',')
			gas_value=float(fields[5])
			date=time.gmtime(float(fields[6]))
			if len(fields)==7:
				if (date.tm_year==2018 and date.tm_mon==1):
					yield((date.tm_mday),(1,gas_value))
		except:
			pass
	def combiner(self,key,values):
		count_trnx=0
		total_gas=0
		for val in values:
			count_trnx += val[0]
			total_gas = val[1]
		yield(key,(count_trnx,total_gas))
	def reducer(self,key,values):
		count_trnx=0
		total_gas=0
		for val in values:
			count_trnx += val[0]
			total_gas = val[1]
		yield(key,(count_trnx,total_gas))

if __name__ == '__main__':
	fork_chain.JOBCONF = {'mapreduce.job.reduces':'4'}
	fork_chain.run()
