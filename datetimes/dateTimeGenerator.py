import random
import datetime
from calendar import monthrange
class DateTimeGenerator():
    '''Generator of datetime objects'''
    def gen_dates(self,samples):
        '''Generates a random sample of datetimes, which is skewed towards summer months, weekends, midday hours, and has a increase in 2024 over 2023'''
        months = random.choices(range(1,13),[2,2,4,3,4,7,8,8,6,5,3,2],k=samples)
        years = random.choices([2023,2024],[3,4],k=samples)
        def get_day_weights(month, year):
            #Gets the first day of the month and the length of the month in days
            weekday, month_days = monthrange(year,month)
            weekWeights = [2,2,2,3,6,5,4]
            #shift the weights over based on the first day of the month
            shifted = [weekWeights[(i+weekday)%7] for i in range(7)]
            weights = []
            #loop the week weights for the whole month
            for day in range(month_days):
                weights.append(shifted[day%7])
            return weights
        hours = random.choices(range(24),[1,.1,.1,.1,1,1,2,3,4,5,5,6,6,6,6,7,7,7,6,5,4,3,2,1],k=samples)
        minutes = random.choices(range(60),None,k=samples)
        seconds = random.choices(range(60),None,k=samples)
        datetimes = []
        for i in range(samples):
            day = random.choices([d + 1 for d in range(monthrange(years[i],months[i])[1])],get_day_weights(months[i],years[i]))[0]
            #chance to output a none or incorrect date
            chance = random.choices([1,2,3],[98,1,1])[0]
            if chance == 1:
                datetimes.append(datetime.datetime(years[i],months[i],day,hours[i],minutes[i],seconds[i]))
            elif chance == 2:
                datetimes.append(datetime.datetime(1,1,1,0,0,0))
            else:
                datetimes.append(None)
        return datetimes
    def gen_date(self):
        '''returns one datetime with the same distributions as gen_dates'''
        return self.gen_dates(1)[0]
dg = DateTimeGenerator()
ds= dg.gen_date()
print(ds)