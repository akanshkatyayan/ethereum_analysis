from mrjob.job import MRJob
from mrjob.step import MRStep


class PartC(MRJob):
    '''
    Class to evaluate top 10 miners by the size of the block mined. 
    Blocks dataset is used.
    '''
    def mapper1(self, _, line):
        '''
        mapper to split records and check timestamp of each transaction. 
        Create Key as month and year and value as 1 for each transaction
        '''
        try:
            fields = line.split(",")
            if (len(fields) == 9):
                miner_block = fields[2]
                size = int(fields[4])
                yield(miner_block, size)
        
        except:
            pass

    def reducer1(self, key, value):
        '''
        Reducer sums all the individual size values of each distinct miner
        '''
        yield(key, sum(value))

    def mapper2(self, key, size_values):
        """
        Mapper 2 to pass key and values as the new values as no further processing required
        """
        try:
            yield(None, (key, size_values))
        except:
            pass
    
    def reducer2(self, _, size_value):
        """
        Reducer 2 to sort size value on decreasing order and yield top 10 miners based on size
        """
        try:
            sorted_size_values = sorted(size_value, reverse=True, key=lambda x: x[1])
            for miner_size in sorted_size_values[:10]:
                yield(miner_size[0], miner_size[1])
        
        except:
            pass
    
    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
    PartC.run()
