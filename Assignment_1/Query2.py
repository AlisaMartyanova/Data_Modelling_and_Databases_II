###################################################################
# Alisa Martyanova BS18-05                                        #
#                                                                 #
# Query 2                                                         #
#                                                                 #
# For each film create list of actors that took part in that film #
# for each pair in that list add one in the 'actor_table'         #
# output is a table in .csv file                                  #
###################################################################

from pymongo import MongoClient
import csv
import time

start_time = time.time()

#connecting to mongodb
client = MongoClient('localhost', 27017)

actor_table = [[0 for x in range(201)] for y in range(201)]
actor_list = []
actor_table[0][0] = ' '

file = open('Alisa_Martyanova_Query_2.csv', 'w+')

for i in client['dvdrental']['actor'].distinct('actor_id'):
    for document in client['dvdrental']['actor'].find({'actor_id': i}):
        name = document.__getitem__('first_name') + ' ' + document.__getitem__('last_name')

    actor_table[0][i] = name
    actor_table[i][0] = name

for i in client['dvdrental']['film_actor'].distinct('film_id'):
    for document in client['dvdrental']['film_actor'].find({'film_id': i}):
        actor_list.append(document.__getitem__('actor_id'))

    for k in range(0, actor_list.__len__()-1):
        for j in range(k+1, actor_list.__len__()):
            actor_table[actor_list[k]][actor_list[j]] += 1
            actor_table[actor_list[j]][actor_list[k]] += 1

    actor_list.clear()


#export to csv
with file:
    writer = csv.writer(file)

    for row in actor_table:
        writer.writerow(row)

print("See output in \'Alisa_Martyanova_Query_2.csv\' file")
actor_table.clear()
file.close()

end = time.time() - start_time
print('%.3f' % end + ' sec.')