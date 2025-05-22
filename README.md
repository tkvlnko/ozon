# ozon

 ЗАПУСКАЕТСЯ ВСЁ КОМАНДОЙ

 ```bash 
 docker compose up -d --build
 ```
ПЕРЕЗАГРУЖВЕМ ВСЁ КОМАНДОЙ
 ```bash 
 docker compose down -v      
rm -rf ./clickhouse/data/clickhouse-*  # старые метаданные
rm -rf ./zookeeper/data
 ```