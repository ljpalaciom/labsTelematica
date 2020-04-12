from mrjob.job import MRJob

class MRCompany(MRJob):

    def mapper(self, _, line):    
        company, price, current_date = line.split(",")
        yield company, (price, current_date)

                
    def reducer(self, key, values):            
        price_key = lambda value: float(value[0])
        result = {}
        # 1 exercise
        values = list(values)
        date_min_value = min(values, key=price_key)[1] # date min price
        date_max_value = max(values, key=price_key)[1] # date max price   
        result["Dia menor valor"] = date_min_value        
        result["Dia mayor valor"] = date_max_value
        yield key, result

                        
        

if __name__ == '__main__':
    MRCompany = MRCompany.run()

