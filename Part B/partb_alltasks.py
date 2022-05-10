from mrjob.job import MRJob
from mrjob.step import MRStep

class PARTB(MRJob):
	"""
	Combined Class to Implement 
	Job 1 (Initiial Aggregation), 
	Job 2 (Join Transactions and Contracts),
	Job 3 (Top Ten)

	"""
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			#Check if number of fields are 7 => Transaction Dataset
			if len(fields) == 7:
				to_address = fields[2]
				value = int(fields[3])
				# 1 here is and identifier
				yield(to_address, (1,value))
			
			# check if number of fields are 5 -> Contract Dataset
			elif len(fields) == 5:
				address1 = fields[0]
				#2 here is and identifier
				yield(address1, (2,None))
		except:
			pass

	def reducer1(self, key, values):
		tempvar = False
		allvalues = []
		for val in values:
			#if in Transaction Dataset
			if val[0]==1:
				allvalues.append(val[1])
			#if in Contract Dataset
			elif val[0] == 2:
				tempvar = True
		if tempvar:
			# if tempvar is True, it means the address is present in both
			# transaction and contract Dataset, hence is popular
			yield(key, sum(allvalues))

	def mapper2(self, key,value):
		yield(None, (key,value))

	def reducer2(self, _, value):
		sortedvalues = sorted(value, reverse = True, key = lambda x: x[1])
		for val in sortedvalues[:10]:
			yield(val[0], val[1])

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PARTB.run()
