import pyspark

sc = pyspark.SparkContext()

def is_good_line(line):
	try:
        	fields = line.split(',')
        	if len(fields) == 7: 
            		str(fields[2]) 
            		if int(fields[3]) == 0:
	                	return False
        	elif len(fields) == 5: 
            		str(fields[0]) 
        	else:
            		return False
        	return True
    	except:
		return False

transactions = sc.textFile('/data/ethereum/transactions')
contracts = sc.textFile('/data/ethereum/contracts')
clean_trnx = transactions.filter(is_good_line)
clean_contract = contracts.filter(is_good_line)
map_trnx = clean_trnx.map(lambda l:(l.split(',')[2],int(l.split(',')[3])))
agg_reduce_trnx = map_trnx.reduceByKey(lambda a,b:a+b)
map_contract = clean_contract.map(lambda x:(x.split(',')[0],None))
join_trnx_cont = agg_reduce_trnx.join(map_contract)
top10_series = join_trnx_cont.takeOrdered(10,key = lambda m: -m[1][0])


for record in top10_series:
	print("{}:{}".format(record[0],record[1][0]))
