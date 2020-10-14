from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()

data_file = '/Users/edlainezamora/dev/twu/data/spark/data*.csv'

print('Extract Part of the ETL')
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(sdfData.count()))
sdfData.show()

print('Transform Part of the ETL - Sales Amount by Item Type')
sdfData.registerTempTable("sales")
salesByItemType = scSpark.sql('SELECT item_type, SUM(item_price) AS total FROM sales group by item_type;')
salesByItemType.show()

print('Load Part of the ETL')
salesByItemType.coalesce(1).write.format('csv').save('sales_amout_by_item_type.csv')
print('sales_amout_by_item_type.csv created')