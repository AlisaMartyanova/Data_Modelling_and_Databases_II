###########################################################
# Alisa Martyanova BS18-05                                #
#                                                         #
# Script that moves the database from an RDBMS (Postgres) #
# to the NoSQL database (MongoDB) with JavaScript files   #
###########################################################

import json
from pymongo import MongoClient
import psycopg2
import time
import os

start_time = time.time()

# you need to change password and username (if necessary)
db_name = 'dvdrental'
user_name = 'postgres'
password = 'Alisa'
host = 'localhost'
path_to_files = '/home/postgres/json_files/' #path to directory where .json files will be stored

#connecting to postgres
conn = psycopg2.connect(dbname = db_name, user = user_name,
                        password = password, host = host)
#connecting to mongodb
client = MongoClient('localhost', 27017)
client.drop_database('dvdrental')

#put all lines of data into one tuple
tweets = []

cursor = conn.cursor()

#create additional table with merged information for convenient querying in Mongodb

#Additional table for the first query
cursor.execute("""CREATE TABLE merged_q1 as  
(
SELECT customer.first_name, customer.last_name, customer.customer_id, inventory.film_id,
film_category.category_id, rental.rental_date
FROM customer, rental, inventory, film_category
WHERE customer.customer_id = rental.customer_id AND rental.inventory_id = inventory.inventory_id
AND inventory.film_id = film_category.film_id
AND extract(year from rental.rental_date) = (select max(extract(year from rental.rental_date)) from rental)
ORDER BY customer.customer_id
);""")

#Additional table for the third and fourth queries
cursor.execute("""CREATE TABLE merged_q3 as
(
SELECT customer.customer_id, inventory.film_id, film.title, film_category.category_id, category.name, rental.rental_date
FROM customer, rental, inventory, film_category, category, film
WHERE customer.customer_id = rental.customer_id AND rental.inventory_id = inventory.inventory_id
AND inventory.film_id = film_category.film_id AND film_category.category_id = category.category_id
AND film.film_id = inventory.film_id
ORDER BY inventory.film_id
);""")

cursor.execute("""SELECT table_name FROM information_schema.tables
 WHERE table_type='BASE TABLE' AND table_schema = 'public'""")

for table in cursor.fetchall():
    name_t = str(table)
    name = name_t[2:len(table)-4] #name of table

    path = path_to_files + name + '.json'
    command = 'COPY (SELECT ROW_TO_JSON(t) FROM (SELECT * FROM ' + name +\
              ') t) TO \'' + path + '\'' #exporting data in table into json file
    cursor.execute(command)
    # intermideate loading of json files
    for line in open(path, 'r'):
        tweets.append(json.loads(line))

    # fill table in mongodb with data from json file
    client['dvdrental'][name].insert_many(tweets)
    tweets.clear()
    os.remove(path)

end = time.time() - start_time
print('%.3f' % end + ' sec.')