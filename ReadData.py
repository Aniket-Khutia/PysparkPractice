from pyspark.sql import *
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark=SparkSession.builder.appName('ReadCSV').getOrCreate()


# creating own column names

myschema= StructType([
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
data=spark.read.csv(path='C:\\Users\\HP PC\\OneDrive\\Desktop\\Food_DEProj\\indian_food.csv',inferSchema=True,schema=myschema,header=True)

data.show()


