# ozon

запуск
 ```bash 
docker compose up -d --build
 ```

перезагрузка
 ```bash 
docker compose down -v      
rm -rf ./clickhouse/data/clickhouse-*
rm -rf ./zookeeper/data
 ```

для просмотра и отладки таблиц в дбивере

 ```bash 
docker exec -it clickhouse-1 clickhouse-client --user default --password default --query "SELECT _shard_num, count() FROM item_upload.attempt_create_time_all GROUP BY _shard_num"\n
 ```

 ```bash 
docker exec -it clickhouse-4 clickhouse-client --user default --password default --query "SELECT * FROM item_upload.attempt_create_time_all LIMIT 10 FORMAT Vertical"
 ```
 ```bash  
 docker exec -it clickhouse-4 clickhouse-client --user default --password default --query "SELECT * FROM item_upload.attempt_create_time_all LIMIT 10"
 ```
 ```bash 
docker exec -it clickhouse-4 clickhouse-client --user default --password default --query "SELECT * FROM item_upload.company_statistic_all LIMIT 10"
 ```
 ```bash 
docker exec -it clickhouse-4 clickhouse-client --user default --password default --query "SELECT * FROM item_upload.company_statistic_daily_all LIMIT 10"
 ```