import clickhouse_connect

# Подключение к ClickHouse
client = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="")
table = "mydb.test_table"

def get_by_category(category, limit):
    return client.query("SELECT * FROM {table} WHERE category = {category} LIMIT {limit}").result_rows

# Закрытие подключения
client.close()
