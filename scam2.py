from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class Scam2(MRJob):
	def trnx_scam_mapper(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				address1 = fields[2]
				yield address1, (1,0)
			
			else:
				line = json.loads(lines)
				rslt = line["result"]

				for i in rslt:
					record = line["result"][i]
					category = record["category"]
					addresses = record["addresses"]
					status = record["status"]
					for j in addresses:
						yield j, (2,category,status)

		except:
			pass

	def trnx_scam_reducer(self, key, values):
		total_value=0
		category=None
		status = None

		for val in values:
			if val[0] == 1:
				total_value +=  val[0]
			else:
				category = val[1]
				status = val[2]
		if category is not None and status is not None:
			yield (status,category), total_value

	def agg_mapper(self,key,value):
		yield(key,value)
	def agg_reducer(self, key, value):
		yield(key,sum(value))

	def steps(self):
		return [MRStep(mapper = self.trnx_scam_mapper, reducer=self.trnx_scam_reducer,jobconf={'mapreduce.job.reduces': '10'}), MRStep(mapper = self.agg_mapper, reducer = self.agg_reducer,jobconf={'mapreduce.job.reduces': '10'})]

if __name__ == '__main__':
	Scam2.run()
				
				
	
	

