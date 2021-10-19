from mrjob.job import MRJob
import csv

class RetailCount(MRJob):

    def mapper(self, key, line):
        parts = list(csv.reader([line.strip()]))[0]
        # One of the lines in the file is a header line, so avoid it.
        if parts[0] != "InvoiceNo":
            description = parts[2].strip().replace("\"", "")
            if description == "":
                description = "Blank"
            quantity = int(parts[3])
            price = float(parts[5])
            yield description, quantity * price

    def combiner(self, key, amount):
        yield key, sum(amount)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    RetailCount.run()
