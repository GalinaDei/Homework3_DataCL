'''Урок 3. Системы управления базами данных MongoDB и Кликхаус в Python
Установите MongoDB на локальной машине, а также зарегистрируйтесь в онлайн-сервисе.
 https://www.mongodb.com/ https://www.mongodb.com/products/compass
Загрузите данные который вы получили на предыдущем уроке путем скрейпинга сайта с помощью Buautiful Soup в MongoDB и создайте базу данных и коллекции для их хранения.
Поэкспериментируйте с различными методами запросов.
'''

from pymongo import MongoClient
import json

client = MongoClient('mongodb://localhost:27017/')
db = client['boxoffice']
boxoffice_data = db['boxoffice_data']

with open('box_office_data.json', 'r') as f:
        data_json = json.load(f)

for item in data_json:
    boxoffice_data.insert_one(item)

query = {"Name" : {"$gte" : "A", "$lt" : "B"}}
Books_AtoG = boxoffice_data.find(query)
for item in Books_AtoG:
  print(item)

query1 = {"In stock": {"$lt": 5}}
projection = {'id': '0', 'Name': '1', 'In stock':'1'} 
low_quantity = boxoffice_data.find(query1)
for item in low_quantity:
  print(item['Name'], item['In stock'])

query2 = {"Price" : {"$gte" : "£22.00", "$lt" : "£52.65"}}
count = boxoffice_data.count_documents(query2)
print(count)