docker run -d --name clickhouse-server -e CLICKHOUSE_DB=mydb -p 9000:9000 -p 8123:8123 -p 9009:9009 clickhouse/clickhouse-server
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
