/user/hive/warehouse

#hive_script_example.hql
CREATE DATABASE IF NOT EXISTS tutorial_db;

USE tutorial_db;

CREATE TABLE IF NOT EXISTS  tutorial_db.hive_script_test
(  id INT,
   technology String,
   type String
)
ROW FORMAT delimited
FIELDS TERMINATED BY '\t'
STORED AS textfile;

LOAD DATA LOCAL inpath '/path_to_script/hive_table_data.txt' OVERWRITE INTO TABLE tutorial_db.hive_script_test;

CREATE TABLE hiveFirstTable
(
   order_id INT,
   order_date STRING,
   cust_id INT,
   order_status STRING
)
ROW FORMAT delimited
--ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS textfile
--STORED AS ORC
tblproperties("skip.header.line.count"="1", "skip.footer.line.count"="1");

CREATE EXTERNAL TABLE hiveFirstExternalTable
(
   order_id INT,
   order_date STRING,
   cust_id INT,
   order_status STRING
)
ROW FORMAT delimited
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION '/user/username/somedirectory';

CREATE TABLE hiveFirstTable_copy  LIKE hiveFirstTable;
CREATE TABLE hiveFirstTable_copy_with_date  AS SELECT * FROM  hiveFirstTable;

LOAD DATA local inpath '/Users/username/data_files/order.txt' OVERWRITE INTO TABLE hiveTempTable;
--INSERT OVERWRITE TABLE hiveFirstTable SELECT order_id,order_data,cust_id,order_status FROM hiveTempTable;
INSERT OVERWRITE TABLE hiveFirstPartitionedClusteredTable PARTITION(order_status) SELECT order_id,order_date,cust_id,order_status FROM hiveTempTable;

INSERT OVERWRITE LOCAL DIRECTORY '/Users/username/Desktop/hive' (SELECT * FROM order_json);
INSERT OVERWRITE DIRECTORY 'hdfs://localhost:9000/user/username/hive' (SELECT * FROM order_json);


LOAD DATA local inpath '/path_to_file/data_files/order_incr_2.txt' OVERWRITE INTO TABLE orders_temp;

INSERT TABLE orders_temp
SELECT orders_incr.order_id, orders_incr.cust_id, orders_incr.order_status, orders_incr.order_date
FROM orders_incr
LEFT OUTER JOIN orders_temp ON (orders_incr.order_id = orders_temp.order_id )
WHERE orders_incr.order_date IN (SELECT distinct order_date FROM orders_temp)
AND orders_temp.order_id IS NULL;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE orders_incr PARTITION(order_date)
SELECT order_id,cust_id,order_status,order_date
FROM orders_temp;

>>$ hive -f /path_to_script/hive_script_example.hql

#hive_script_passing_variables.hql
SELECT * FROM tutorial_db.hive_script_test WHERE type='${dbtype}';

>>$hive -hivevar var_dbtype='RDBMS' -f /path_to_script/hive_script_passing_variables.hql

#hive_script_passing_variables.hql
SELECT * FROM tutorial_db.hive_script_test WHERE type='${var_dbtype}' AND technology='${var_tech}';

>>$ hive -hivevar var_dbtype='RDBMS' -hivevar var_tech='Hive' -f /path_to_script/hive_script_passing_multi_variables.hql

#Executing queries without login into Hive console
>>$ hive -e 'SELECT * FROM tutorial_db.hive_script_test limit 10'

#Setting and using variables within Hive console
hive> set dbtype='RDBMS';
hive> SELECT * FROM tutorial_db.hive_script_test WHERE type=${hiveconf:dbtype};