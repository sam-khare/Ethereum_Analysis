from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class Scam1(MRJob):
	def trnx_scam_mapper(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				address1 = fields[2]
				value = float(fields[3])
				yield address1, (value,0)
			
			else:
				line = json.loads(lines)
				rslt = line["result"]

				for i in rslt:
					record = line["result"][i]
					category = record["category"]
					addresses = record["addresses"]

					for j in addresses:
						yield j, (category,1)

		except:
			pass

	def trnx_scam_reducer(self, key, values):
		total_value=0
		category=None

		for k in values:
			if k[1] == 0:
				total_value +=  k[0]
			else:
				category = k[0]
		if category is not None:
			yield (category, total_value)

	def agg_mapper(self,key,value):
		yield(key,value)
	def agg_reducer(self, key, value):
		yield(key,sum(value))

	def steps(self):
		return [MRStep(mapper = self.trnx_scam_mapper, reducer=self.trnx_scam_reducer,jobconf={'mapreduce.job.reduces': '10'}), MRStep(mapper = self.agg_mapper, reducer = self.agg_reducer,jobconf={'mapreduce.job.reduces': '10'})]

if __name__ == '__main__':
	Scam1.run()
				
				
	
	

