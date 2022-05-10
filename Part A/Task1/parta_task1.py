from mrjob.job import MRJob
import time


class PartA(MRJob):
    '''
    Class to get number of Ethereum transactions occurring every month each year.
    '''
    def mapper(self, _, line):
        '''
        mapper to split records and check timestamp of each transaction. 
        Create Key as month and year and value as 1 for each transaction
        '''
        try:
            fields = line.split(",")
            if (len(fields) == 7):
                epoch_time=int(fields[6]) # Timestamp value
                date_year = time.strftime("%Y", time.gmtime(epoch_time))
                date_month = time.strftime("%m", time.gmtime(epoch_time))
                yield((date_year, date_month), 1)
        except:
            pass

    def combiner(self, key, value):
        yield(key, sum(value))

    def reducer(self, key, value):
        '''
        Reducer sums all the individual transactions occurring on same month and year
        '''
        yield(key, sum(value))


if __name__ == '__main__':
    PartA.run()
