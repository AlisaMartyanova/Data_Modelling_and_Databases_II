##############################################################
# Alisa Martyanova BS18-05                                   #
#                                                            #
# Query 4                                                    #
#                                                            #
# Recommendation is based on the most popular films          #
# (top 5 films that were mostly rented) of the same category #
# that customer rented in particular date                    #
##############################################################

from pymongo import MongoClient
import time

#connecting to mongodb
client = MongoClient('localhost', 27017)


file = open("Alisa_Martyanova_Query_4.txt","w+")
films = {}

print("Enter the customer id (number from 1 to 599):")
customer_id = int(input())

#all possible rental dates of chosen customer
for document in client['dvdrental']['rental'].find({'customer_id': customer_id}):
    print(document.__getitem__('rental_date'))

print("\nChoose the date of rental and enter it: ")
date = str(input())

start_time = time.time()

#create collection with films and number of times they have been rented
collection = client["dvdrental"]["film_rented_num"]

for i in client['dvdrental']['film'].distinct('film_id'):
    count = 0
    for document in client['dvdrental']['merged_q3'].find({'film_id': i}):
        count +=1

    dict = {"film": document.__getitem__('title'), "category": document.__getitem__('category_id'), "rented_num": count}
    collection.insert_one(dict)


#find category of film that customer rented in chosen date
for document in client['dvdrental']['merged_q3'].find({'customer_id': customer_id, 'rental_date': date}):
    cat_id = document.__getitem__('category_id')
    film_title = document.__getitem__('title')

#find films of the same category
for document in client['dvdrental']['film_rented_num'].find({'category': cat_id}):
    if (not document.__getitem__('film')  == film_title):  #check that it is not the same film that customer rented
        films.__setitem__(document.__getitem__('rented_num'), document.__getitem__('film'))

for document in client['dvdrental']['customer'].find({'customer_id': customer_id}):
    name = document.__getitem__('first_name')
    surname = document.__getitem__('last_name')

file.write("Top 5 recommended films for %s " % name)
file.write("%s " % surname)
file.write("\n(for date: %s) \n\n" % date)

#chose 5 mostly rented films of that category
for i,j in zip(sorted(films.keys(),reverse=True), range(1,6)):
    file.write("%d. " % j)
    file.write("\"%s\" " % films[i])
    file.write("(has been rented %d times)\n\n" % i)


print("See output in \'Alisa_Martyanova_Query_4.txt\' file")
films.clear()
file.close()

end = time.time() - start_time
print('%.3f' % end + ' sec.')