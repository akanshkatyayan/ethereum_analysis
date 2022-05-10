from mrjob.job import MRJob
import time


class GG(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                gas = int(fields[5])
                date = time.gmtime(float(fields[6]))
                key = str(date.tm_year) + '-' + str(date.tm_mon)
                yield(key,gas)
        except:
            pass

    def reducer(self, key, values):
        price = 0
        count = 0

        for val in values:
            price = price + val
            count = count + 1

        yield(key, float(price/count))


if __name__ == '__main__':
    GG.run()
