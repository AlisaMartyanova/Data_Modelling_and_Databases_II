###############################################################################
# Alisa Martyanova BS18-05                                                    #
#                                                                             #
# Query 5                                                                     #
#                                                                             #
# Create adjacency matrix of actors based on Query 2                          #
# Using Dijkstra algorithm find shortest path from chosen actor to all others #
###############################################################################

from pymongo import MongoClient
import csv
import sys
import time


print("Enter the customer id (number from 1 to 599):")
customer_id = int(input())

start_time = time.time()

#connecting to mongodb
client = MongoClient('localhost', 27017)

output = [[0 for x in range(201)] for y in range(2)]
output[0][0] = ' '

for document in client['dvdrental']['actor'].find({'actor_id': customer_id}):
    name = document.__getitem__('first_name') + ' ' + document.__getitem__('last_name')
    output[1][0] = name


for i in client['dvdrental']['actor'].distinct('actor_id'):
    for document in client['dvdrental']['actor'].find({'actor_id': i}):
        name = document.__getitem__('first_name') + ' ' + document.__getitem__('last_name')

    output[0][i] = name




adjacency_matrix = [[0 for x in range(200)] for y in range(200)]
actor_list = []

file = open('Alisa_Martyanova_Query_5.csv', 'w+')


for i in client['dvdrental']['film_actor'].distinct('film_id'):
    for document in client['dvdrental']['film_actor'].find({'film_id': i}):
        actor_list.append(document.__getitem__('actor_id'))

    for k in range(0, actor_list.__len__()-1):
        for j in range(k+1, actor_list.__len__()):
            adjacency_matrix[actor_list[k] - 1][actor_list[j] - 1] = 1
            adjacency_matrix[actor_list[j] - 1][actor_list[k] - 1] = 1

    actor_list.clear()


def minDistance(dist, sptSet):
    min = sys.maxsize

    for v in range(0, 200):
        if (sptSet[v] == False and dist[v] <= min):
            min = dist[v]
            min_index = v

    return min_index

def dijkstra (graph, src):
    dist = [sys.maxsize]*200
    sptSet = [False]*200
    dist[src] = 0


    for count in range(0, 199):
       u = minDistance(dist, sptSet)
       sptSet[u] = True

       for v in range(0, 200):
           if (sptSet[v] == False and
                   graph[u][v] > 0 and
                   dist[u] + graph[u][v] < dist[v]):

               dist[v] = dist[u] + graph[u][v]


    for h in range(0,200):
        output[1][h+1] = dist[h]


dijkstra(adjacency_matrix, customer_id-1)

with file:
    writer = csv.writer(file)

    for row in output:
        writer.writerow(row)

print("See output in \'Alisa_Martyanova_Query_5.csv\' file")
adjacency_matrix.clear()
file.close()

end = time.time() - start_time
print('%.3f' % end + ' sec.')