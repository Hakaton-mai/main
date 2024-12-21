import clickhouse_connect

# Подключение к ClickHouse
client = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="")

# SQL-запрос для получения данных
query = "SELECT * FROM mydb.test_table LIMIT 10"
result = client.query(query)

# Вывод данных
for row in result.result_rows:
    print(row)

# Закрытие подключения
client.close()
