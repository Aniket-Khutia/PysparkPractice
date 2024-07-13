from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName('Transformations').getOrCreate()

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


# TRANSFORMATION 1: ADD A NEW COLUMN TOTAL=PREP+COOK

print('------------------------Trans1-----------------------------------')
df=data.select(col('name'),col('ingred'),col('flavor'),col('course'),col('state'),col('region'),col('cook'),col('prep'),(col('cook')+col('prep')).alias('total_time'))


# TRANSFORMATION 2: REMOVE RECORDS HAVING A -VE COOK+PREP TIME

print('------------------------Trans2-----------------------------------')

dfclean=df.filter((col('total_time')>0) & (col('cook')>0) & (col('prep')>0))
dfclean.show(dfclean.count(),truncate=False)

# # TRANSFORMATION 3: SELECTING THE ITEMS WHICH ARE LESSER THAN THE AVG TOTAL TIME
# print('------------------------Trans3-----------------------------------')

#
# condition=dfclean.agg(round(avg(col('total_time')),2)).collect()[0][0]
# print(condition)
# dfclean.select('*').filter(expr('total_time')<condition).show(dfclean.count())
#
#
# # TRANSFORMATION 4: MAKING A SEPARATE DF FOR THE BAD RECORDS
#
# badDF=df.select('*').filter((col('cook')<0) & (col('prep')<0))
# badDF.show(badDF.count())
#
#
# # TRANSFORMATION 5: MAKING UNION OF 2 DATAFRAMES
#
# dfclean.union(badDF).show()

# TRANSFORMATION 6: MAKING A DATAFRAME WITH COLUMN 'FEASIBLE' DEPENDING UPON THE TOTAL_TIME COLUMN

# print('------------------------Trans6-----------------------------------')
#
# newDF=dfclean.withColumn('feasible',when(col('total_time')<=35,'high').
#                          when((col('total_time')>35) & (col('total_time')<=60),'med').
#                          otherwise('low'))
# newDF.orderBy(col('total_time')).show(newDF.count())
#
#
# # TRANSFORMATION 7: MAKING A COUNT OF FOOD ITEMS GROUPING BY THEIR REGION
#
# print('--------------------------------Trans7------------------------------')
#
# dfclean.groupBy('region','state').agg(count(col('name'))).sort(col('region')).show()
