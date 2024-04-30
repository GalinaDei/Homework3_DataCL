'''Зарегистрируйтесь в ClickHouse.
Загрузите данные в ClickHouse и создайте таблицу для их хранения.'''
from clickhouse_driver import Client
import json

client = Client('localhost')

client.execute('DROP DATABASE IF EXISTS boxoffice')
client.execute('CREATE DATABASE IF NOT EXISTS boxoffice')

client.execute('''
    CREATE TABLE IF NOT EXISTS boxoffice.boxoffice_data (
    id UInt64,
    name String,
    price String,
    in_stock UInt64,
    description String
    ) ENGINE = MergeTree()
    ORDER BY id
    ''')
''

print("Таблица создана.")
with open('box_office_data.json', 'r') as file:
    data = json.load(file)

id = 0
for item in data:
    try:
        client.execute("""
            INSERT INTO boxoffice.boxoffice_data (
            id, name, price,
            in_stock, description
            ) VALUES""",
            [(id,
            item['Name'] or "",
            item['Price'] or "",
            item['In stock'] or "",
            item['Description'] or ''
            )])
        id += 1
    except Exception as ex:
        print("Ошибка ввода данных:", ex)
        continue

print("Данные введены успешно.")

result = client.execute("SELECT * FROM boxoffice.boxoffice_data")
print("Всего записей:", len(result))

print("Вставленная запись:", result[0])