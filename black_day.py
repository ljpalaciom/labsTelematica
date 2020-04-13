from mrjob.job import MRJob, MRStep

# 3 exercise
class MRCompany(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_date_min_price),
            MRStep(reducer=self.reducer_black_day)
        ]

    def mapper(self, _, line):
        if len(line) > 0 and line != "company,price,date":
            company, price, current_date = line.split(",")
            yield company, (price, current_date)

    def reducer_date_min_price(self, key, values):
        price_key = lambda value: float(value[0])
        date_min_value = min(values, key=price_key)[1]  # date min price
        yield "date", date_min_value

    def reducer_black_day(self, key, values):
        # 3 exercise
        values = list(values)
        count = {date: values.count(date) for date in values}
        black_day = max(count, key=count.get)
        yield "black_day", black_day


if __name__ == '__main__':
    MRCompany = MRCompany.run()
