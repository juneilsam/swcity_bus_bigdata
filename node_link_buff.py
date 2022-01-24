# 필요한 라이브러리, 패키지
import pyspark.sql as ps
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pyspark
import findspark
findspark.init('/opt/spark')

import pandas as pd
import geopandas as gpd

import sedona
from sedona.register import SedonaRegistrator  
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# 스파크 사용을 위한 세션 빌더
spark = SparkSession.builder.appName("suwon") \
.config("hive.metastore.uris", "thrift://") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator",SedonaKryoRegistrator.getName) \
    .config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:1.1.0-24.1').enableHiveSupport().getOrCreate()

# sedona 사용을 위한 register
SedonaRegistrator.registerAll(spark)

# 도로의 string 좌표 원본 데이터를 불러옴
node_sql = spark.sql("SELECT LINK_ID, LANES, MAX_SPD, ST_GeomFromText(s_node.geometry) as geom from row_link_test_1 as s_node")

# 제한 속도별로 데이터를 분할하기 위해 distinct로 값을 만듦
spd_collect = node_sql.select('MAX_SPD').distinct().collect()

for j in spd_collect:
    max_spd = j[0]
    # 같은 제한 속도를 가진 도로 링크로 테이블 분할
    node_df  = node_sql.filter(node_sql.MAX_SPD == max_spd)
    
    # 쿼리문으로 불러온 데이터를 pandas 데이터 프레임 형식으로 불러옴
    node_df = gpd.GeoDataFrame(node_df.toPandas(), geometry='geom')
    
    # buffer 기능을 이용, string 좌표를 polygon으로 변환
    node_df['geom'] = node_df['geom'].buffer((node_df.LANES * 0.00027), cap_style=2)
    node_df = spark.createDataFrame(node_df)
    
    # polygon으로 변환한 데이터 추가
    node_df = node_df.withColumn("geom", F.col("geom").cast('string'))
    node_df.createOrReplaceTempView('node_df')
    
    # hive에 업로드
    node_df.write.mode("append").insertInto("row_link_test_3")
    del node_df
