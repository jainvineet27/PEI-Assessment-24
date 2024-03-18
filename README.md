This is a PEI Assessment
Ingestion 
Transformation 
Load

We have ingested the data, the data found to be in all three different formats
Configurations are uploaded to make the parsing and date conversion in Spark version >= 3.0
We have done bunch of transformations : - 
missing nulls  hence done their removal and showed original and new count 
cleaning of data incuding phone 
performing cleaning and make it normalized column such as phone 
Performed join operation 
However I assume the pyspark queries needs to be translated back to spark sql 
queries. 

Only loading 
customer_df.write\
.mode('overwrite')\
.format('parquet')\
  .partitionBy('city')\
  .option('path','product')\
  .save()

  product.write\
  .format('parquet')\
  .mode('overwrite')\
    .partitionBy('state')\
  .option('path','product')\
  .save()


  product.write\
  .format('parquet')\
  .mode('overwrite')\
    .partitionBy('order_date')\
    .bucketBy(4,'customer_id')\
  .option('path','product')\
  .save()

  
  
