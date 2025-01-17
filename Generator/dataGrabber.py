import csv 
import os
import random

def get_data_by_country(country, file, nameSeed):
    file = open(file, "r", encoding='utf-8')
    recordList = []
    exitMarker = False
    # go through csv
    for record in csv.reader(file):
        #if country is detected then start adding to a list
        if country in record[0]:
            recordList.append(record[1])
            exitMarker = True
        #if program has read through all records for that country then bail
        elif exitMarker == True and country not in record[0]:
            break
    if len(recordList) == 0:
        recordList = get_data_by_country("United States", file, nameSeed)
    random.seed(nameSeed)
    randomIndex = random.randint(0, len(recordList)-1)
    file.close()
    return recordList[randomIndex]

def get_product(fileName, index):
    file = open(fileName, "r", encoding='utf-8')
    product = []
    currIndex = 0
    for record in csv.reader(file):
        if currIndex == index:
            return record
        currIndex += 1 

'''
sampleSeed = 476
country = "United States"

lastName = get_data_by_country(country, 'filtered-last-names.csv', sampleSeed)
firstName = get_data_by_country(country, "filtered-first-names.csv", sampleSeed)

print(firstName + " " + lastName)

city = get_data_by_country(country, "filtered-cities.csv", sampleSeed)

print(city)

print(get_product("Camping-Products.csv", 5)) '''