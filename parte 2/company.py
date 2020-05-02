from mrjob.job import MRJob

# 1 exercise
class MRCompany(MRJob):

    def mapper(self, _, line):    
        if len(line) > 0 and line != "company,price,date":
            company, price, current_date = line.split(",")
            yield company, (price, current_date)
                
    def reducer(self, key, values):            
        price_key = lambda value: float(value[0])
        result = {}
        values = list(values)
        min_value = min(values, key=price_key) # date min price
        max_value = max(values, key=price_key) # date max price   
        result["Menor valor y fecha"] = min_value                
        result["Mayor valor y fecha"] = max_value
        yield key, result
                            

if __name__ == '__main__':
    MRCompany = MRCompany.run()

