import findspark
findspark.init()
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import sedona
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.register import SedonaRegistrator
import warnings
from datetime import timedelta

spark = SparkSession.builder.appName('spark'). \
                            config("spark.serializer", KryoSerializer.getName). \
                            config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
                            config('spark.jars.packages',
                                'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:1.1.0-24.1') \
                            .enableHiveSupport().getOrCreate()

SedonaRegistrator.registerAll(spark)

yesterday = datetime.today().date() - timedelta(days=1)

node_sql = spark.sql("SELECT LINK_ID, MAX_SPD, ST_GeomFromWKT(low_link.geom) as geometry from low_link")

roids = spark.sql(f"SELECT gnssDF.BUS_ROUTE_ID as ROIDS from dp_gnss as gnssDF where partition='{yesterday}'").select('ROIDS').distinct().collect()

print("start :", datetime.now())

for i in roids:
    start = datetime.now()
    roid = i[0]
    print(roid)

    if roid == 'None':
        pass

    else:
        gnss_sql = spark.sql(f"SELECT gnssDF.DEVICE_KEY, gnssDF.MDT_ID, gnssDF.BUS_NO, gnssDF.BUS_ROUTE_ID as ROUTE_ID, gnssDF.VH_ID, gnssDF.BUS_COMPANY, gnssDF.BUS_ROUTE_NO as ROUTE_NO, gnssDF.COLLECTION_DATE, \
            gnssDF.SPEED, gnssDF.LONGITUDE, gnssDF.LATITUDE, gnssDF.ANGLE, ST_POINT(LONGITUDE, LATITUDE) AS geom FROM dp_gnss as gnssDF where BUS_ROUTE_ID = '{roid}' and partition='{yesterday}'")

        gnss_sql = gnss_sql.filter((gnss_sql.SPEED > 30) & (gnss_sql.SPEED < 180))
        gnss_sql.createOrReplaceTempView('df_speed')
        node_sql.createOrReplaceTempView('node_df1')

        try:
            temp_df1 = spark.sql("select DEVICE_KEY, MDT_ID, VH_ID, BUS_NO, BUS_COMPANY, LATITUDE, LONGITUDE, ANGLE, \
                    LINK_ID, SPEED, ROUTE_NO, node_df1.MAX_SPD, COLLECTION_DATE, ROUTE_ID from node_df1, df_speed WHERE ST_Within(df_speed.geom, node_df1.geometry) and (SPEED > (MAX_SPD + 20))")

            if temp_df1.select("SPEED").take(1)[0][0] != 'None':
                del gnss_sql
                temp_df2 = temp_df1["DEVICE_KEY", "MDT_ID", "VH_ID", "BUS_NO", "BUS_COMPANY", "LATITUDE", "LONGITUDE", "ANGLE", \
                    "LINK_ID", "SPEED", "ROUTE_ID", "ROUTE_NO", "MAX_SPD", "COLLECTION_DATE"].withColumn('WORK_DATE', F.lit(datetime.today().date()))
                temp_df1 = temp_df1.dropDuplicates(['DEVICE_KEY', 'COLLECTION_DATE'])
                temp_df2 = temp_df2.dropDuplicates(['DEVICE_KEY', 'COLLECTION_DATE'])
                temp_df2.write.mode("append").insertInto("MD_SPEED") # , overwrite=False)
                temp_df1.write.mode("append").insertInto("TRACK_SPEED") # , overwrite=False)
                del temp_df2
                del temp_df1
            else:
                print("else-pass")
                pass
        except:
            print("try-pass")
            pass
    finish = datetime.now()
    print(finish - start)
print("finish :", datetime.now())
