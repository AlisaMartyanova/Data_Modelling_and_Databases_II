###################################################################################################
# Alisa Martyanova BS18-05                                                                        #
#                                                                                                 #
# Query 1                                                                                         #
#                                                                                                 #
# For each customer create a list of unique categories of rented films for current year           #
# if length of the list is more than one it means that customer rented films                      #
# of two or more different categories and that's why we include this customer in the output lists #
###################################################################################################

from pymongo import MongoClient
import time
import csv

start_time = time.time()

#connecting to mongodb
client = MongoClient('localhost', 27017)

output = [[0 for x in range(3)] for y in range(600)]
output[0][0] = 'First name'
output[0][1] = 'Last name'
output[0][2] = 'Customer id'

len = 1

file = open("Alisa_Martyanova_Query_1.csv","w+")

for i in client['dvdrental']['merged_q1'].distinct('customer_id'):
    array = []
    for document in client['dvdrental']['merged_q1'].find({'customer_id': i}):

        cat_id = document.__getitem__('category_id')

        if (not array.__contains__(cat_id)):
            array.append(cat_id)

    if (array.__len__()>1):
        output[len][0] = document.__getitem__('first_name')
        output[len][1] = document.__getitem__('last_name')
        output[len][2] = document.__getitem__('customer_id')
        len += 1

#export to csv
with file:
    count = 0
    writer = csv.writer(file)

    for row in output:
        if count < len:
            writer.writerow(row)
            count += 1

print("See output in \'Alisa_Martyanova_Query_1.csv\' file")

file.close()


end = time.time() - start_time
print('%.3f' % end + ' sec.')