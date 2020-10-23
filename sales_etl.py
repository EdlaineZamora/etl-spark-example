from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("Reading csv files") \
        .getOrCreate()

data_file = './data-set/data*.csv'

print('ETL (Extract Part)')
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(sdfData.count()))
sdfData.show()

print('ETL (Transform Part) - Aggregating Sales Amount by Item Type')
sdfData.registerTempTable("sales")
salesByItemType = scSpark.sql('SELECT item_type, SUM(item_price) AS total FROM sales group by item_type;')
salesByItemType.show()

print('ETL (Load Part)')
salesByItemType.coalesce(1).write.format('csv').save('sales_amout_by_item_type.csv')
print('sales_amout_by_item_type.csv generated')