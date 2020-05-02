from mrjob.job import MRJob
from datetime import datetime

# 2 exercise
class MRCompany(MRJob):

    def mapper(self, _, line):  
        if len(line) > 0 and line != "company,price,date":
            company, price, current_date = line.split(",")
            yield company, (price, current_date)
                
    def reducer(self, key, values):                    

        format_date = "%Y-%m-%d"        
        values_sorted = sorted(values, key=lambda value: datetime.strptime(value[1], format_date)) # Ascending order by date
        values_sorted = list(map(lambda value: (float(value[0]), value[1]), values_sorted))
        if all(values_sorted[i][0] <= values_sorted[i + 1][0] for i in range(len(values_sorted)-1)):
            yield key, []                       

if __name__ == '__main__':
    MRCompany = MRCompany.run()

