from mrjob.job import MRJob
from mrjob.step import MRStep

class top_miners(MRJob):
	def miner_agg_mapper(self,_ ,line):
		fields = line.split(',')
		try:
			if len(fields)== 9:
				miner = fields[2]
				blk_size = fields[4]
				yield(miner,int(blk_size))
			
		except:
  			pass      
	def miner_agg_reducer(self,miner,blk_size):
		yield(miner,sum(blk_size))
	def top_miner_mapper(self,miner,block_sum):
		yield(None,(miner,block_sum))

	def top_miner_reducer(self,_ ,values):
		sorted_values = sorted(values, reverse = True, key = lambda tup : tup[1])
		i=0
		for val in sorted_values:
			yield(val[0],val[1])
			i += 1
			if i >= 10:
				break
	
	def steps(self):
 		return [MRStep(mapper = self.miner_agg_mapper,reducer=self.miner_agg_reducer),MRStep(mapper=self.top_miner_mapper,reducer=self.top_miner_reducer)]
		

if __name__=='__main__':
	top_miners.run()
