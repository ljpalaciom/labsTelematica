from mrjob.job import MRJob
from datetime import datetime
class MRCompany(MRJob):

    def mapper(self, _, line):    
        company, price, current_date = line.split(",")
        yield company, (price, current_date)
                
    def reducer(self, key, values):                    
        # 2 exercise
        format_date = "%Y-%m-%d"        
        values_sorted = sorted(values, key=lambda value: datetime.strptime(value[1], format_date)) # Ascending order by date
        if all(values_sorted[i][0] <= values_sorted[i + 1][0] for i in range(len(values_sorted)-1)):
            yield key, []                        

if __name__ == '__main__':
    MRCompany = MRCompany.run()

