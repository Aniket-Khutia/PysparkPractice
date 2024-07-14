from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
# import pip
# pip.main(['install','pandas'])
import pandas as pd

spark=SparkSession.builder.appName('Joins').getOrCreate()

INPUTSCHEMA= StructType([
    StructField('name',StringType(),True),
    StructField('ingred',StringType(),True),
    StructField('diet',StringType(),True),
    StructField('prep',IntegerType(),True),
    StructField('cook',IntegerType(),True),
    StructField('flavor',StringType(),True),
    StructField('course',StringType(),True),
    StructField('state',StringType(),True),
    StructField('region',StringType(),True)
])
data=spark.read.csv(path='C:\\Users\\HP PC\\OneDrive\\Desktop\\Food_DEProj\\indian_food.csv',inferSchema=True,schema=INPUTSCHEMA,header=True)
#data.show()

data2=spark.read.csv(path='C:\\Users\\HP PC\\OneDrive\\Desktop\\Pyspark Practice\\indian_food_clean_2.csv',inferSchema=True,header=True)

df=data.select(col('name'),col('ingred'),col('flavor'),col('diet'),col('course'),col('state'),col('region'),col('cook'),col('prep'),(col('cook')+col('prep')).alias('total_time'))
dfclean=df.filter((col('total_time')>0) & (col('cook')>0) & (col('prep')>0))
# dfclean.show(dfclean.count(),truncate=False)

# data2.show(data2.count())

# print('No of records in dfclean= ',dfclean.count())
# print('No of records in data2= ',data2.count())


# JOINING THE 2 TABLES BASED ON NAME COLUMN


combined_cleandata=dfclean.join(data2,dfclean.name==data2.name,how='inner').select('food_id',dfclean.name,dfclean.ingred,dfclean.diet,dfclean.course,dfclean.state,
                                                                                   dfclean.region,'total_time','flavor_profile','no_of_ingred')
print('No of records of combined df= ',combined_cleandata.count())
combined_cleandata.show(combined_cleandata.count())


#combined_cleandata.write.csv('C:\\Users\\HP PC\\OneDrive\\Desktop\\Pyspark Practice\\Freshfooddata',mode='overwrite',header=True)
