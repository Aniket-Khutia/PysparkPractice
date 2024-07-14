from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

import random
spark=SparkSession.builder.appName('WindowFunctions').getOrCreate()

data=spark.read.csv("C:\\Users\\HP PC\\OneDrive\\Desktop\\Pyspark Practice\\FreshFoodData.csv",header=True,inferSchema=True)

#data.show(data.count())

# TRANSFORMATION 1: CREATE A WINDOW ON REGION AND DISPLAY THE MINIMUM TOTAL_TIME FOR EACH RECORD HAVING THE SAME REGION

window=Window.partitionBy(col('region')).orderBy(col('state'))
df=data.withColumn('MinTime',min(col('total_time')).over(window))
df.show(df.count())

# TRANSFORMATION 2: RANKING THE FOODS FOR EACH STATE IN EACH REGION FOR EACH DIET

window=Window.partitionBy(col('region'),col('state'),col('diet')).orderBy(col('total_time'))
df=data.select('food_id','name','region','state','diet','total_time','ratings').withColumn('rankbytime',dense_rank().over(window))
df.show(df.count())
print(df.count())


# TRANSFORMATION 3: RANKING FOOD ITEM OF EACH STATE BY RATING

window=Window.partitionBy(col('state')).orderBy(desc(col('ratings')))
dfrank_by_ratings=df.select('state','name','ratings').withColumn('rankbyrating',dense_rank().over(window))

dfrank_by_ratings.show(dfrank_by_ratings.count())








# Show the resulting DataFrame



