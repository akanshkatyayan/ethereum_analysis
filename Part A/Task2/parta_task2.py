from mrjob.job import MRJob
import time


class PartATask2(MRJob):
    """
    Average value of transactions in each month between the start and end of the dataset.
    """
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            value = int(fields[3])
            epoch_time = float(fields[6]) #Timestamp
            date_year = time.strftime("%Y", time.gmtime(epoch_time))
            date_month = time.strftime("%m", time.gmtime(epoch_time))
            yield((date_year, date_month), (1, value))
        except:
            pass

    def combiner(self, key, value):
        count = 0
        total_value = 0
        for val in value:
            count += val[0]
            total_value += val[1]
        yield(key, (count, total_value))

    def reducer(self, key, value):
        count = 0
        total_value = 0
        for val in value:
            count += val[0]
            total_value += val[1]
        yield(key, total_value/count)

if __name__ == '__main__':
    PartATask2.run()
