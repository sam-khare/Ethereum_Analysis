import pyspark
import time

sc = pyspark.SparkContext()


def is_good_line_transact(line):
        try:
                fields = line.split(',')
                if len(fields)!= 7:
                        return False
                float(fields[5])
                float(fields[6])
                return True
        except:
                return False

def is_good_line_contract(line):
        try:
                fields = line.split(',')
                if len(fields) != 5:
                        return False
                float(fields[3])
                return True

        except:
                False

def is_good_line_block(line):
        try:
                fields = line.split(',')
                if len(fields)!=9:
                        return False

                float(fields[0])
                float(fields[3])
                float(fields[7])
                return True

        except:
                return False


lines_transaction = sc.textFile('/data/ethereum/transactions')
lines_contract = sc.textFile('/data/ethereum/contracts')
lines_block = sc.textFile('/data/ethereum/blocks')
clean_linestransact = lines_transaction.filter(is_good_line_transact)
clean_linescontract = lines_contract.filter(is_good_line_contract)
clean_linesblock = lines_block.filter(is_good_line_block)

time_trnx = clean_linestransact.map(lambda k: (float(k.split(',')[6]), float(k.split(',')[5])))
date_trnx = time_trnx.map(lambda (a,b): (time.strftime("%y.%m", time.gmtime(a)), (b,1)))
trnx_time = date_trnx.reduceByKey(lambda (a, b), (c, d): (a+c, b+d)).map(lambda j: (j[0], (j[1][0]/j[1][1])))
final1 = trnx_time.sortByKey(ascending=True)
final1.saveAsTextFile('avg_gas')


blocks = clean_linescontract.map(lambda k: (k.split(',')[3], 1))
block_difference = clean_linesblock.map(lambda g: (g.split(',')[0], (int(g.split(',')[3]), int(g.split(',')[6]), time.strftime("%y.%m", time.gmtime(float(g.split(',')[7]))))))
results = block_difference.join(blocks).map(lambda (id, ((a, b, c), d)): (c, ((a,b), d)))
final2 = results.reduceByKey(lambda ((p,q), r) , ((s, t), u): ((p + s, q + t), r+u)).map(lambda h: (h[0], (float(h[1][0][0]/h[1][1]), h[1][0][1]/ h[1][1]))).sortByKey(ascending=True)
final2.saveAsTextFile('time_diff')

