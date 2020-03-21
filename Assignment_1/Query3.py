##########################################################
# Alisa Martyanova BS18-05                               #
#                                                        #
# Query 3                                                #
#                                                        #
# For each film count number of times it has been rented #
##########################################################

from pymongo import MongoClient
import time
import csv

start_time = time.time()
#connecting to mongodb
client = MongoClient('localhost', 27017)

output = [[0 for x in range(3)] for y in range(1001)]

output[0][0] = 'Film title'
output[0][1] = 'Category'
output[0][2] = 'Has been rented'

len = 1

file = open("Alisa_Martyanova_Query_3.csv","w+")

for i in client['dvdrental']['film'].distinct('film_id'):
    count = 0
    for document in client['dvdrental']['merged_q3'].find({'film_id': i}):
        count +=1

    dict = {"film": document.__getitem__('title'), "category": document.__getitem__('category_id'), "rented_num": count}

    output[len][0] = document.__getitem__('title')
    output[len][1] = document.__getitem__('name')
    output[len][2] = count
    len += 1


#export to csv
with file:
    writer = csv.writer(file)

    for row in output:
        writer.writerow(row)

print("See output in \'Alisa_Martyanova_Query_3.csv\' file")
file.close()

end = time.time() - start_time
print('%.3f' % end + ' sec.')