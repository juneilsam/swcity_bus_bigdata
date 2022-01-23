# 필요한 라이브러리, 패키지
import findspark
findspark.init()

from pyspark.sql.session import SparkSession # 스파크
import pyspark.sql.functions as F

from datetime import datetime
from datetime import timedelta

import sedona # 스파크 sedona
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.register import SedonaRegistrator

# 스파크 세션
spark = SparkSession.builder.appName("suwon")\
        .config("hive.metastore.uris", "thrift://")\
        .config("spark.serializer", KryoSerializer.getName)\
        .enableHiveSupport().getOrCreate()

# 파티션을 위한 날짜 기준 설정
yesterday = datetime.today().date() - timedelta(days=1)

gnss_sql = spark.sql(f"SELECT gnssDF.DEVICE_KEY, gnssDF.MDT_ID, gnssDF.BUS_NO, gnssDF.BUS_ROUTE_ID as ROUTE_ID, gnssDF.VH_ID, gnssDF.BUS_COMPANY, gnssDF.BUS_ROUTE_NO as ROUTE_NO, gnssDF.COLLECTION_DATE, \
    gnssDF.SPEED, gnssDF.LONGITUDE, gnssDF.LATITUDE, gnssDF.ANGLE FROM dp_gnss as gnssDF where partition='{yesterday}' and bus_route_id != 'null'")

# 장치 키 고유값 리스트
mac = gnss_sql.select('DEVICE_KEY').distinct().collect()

# 장치 키 기준으로 시속 100이상 버스 판별 
for i in mac:
    device = i[0]
    print(device)
    df_speed1 = gnss_sql.filter(gnss_sql.DEVICE_KEY == device)
    df_speed1 = df_speed1[SPEED > 100]
    df_speed1.withColumn('MAX_SPD', F.lit(100)).write.mode("append").insertInto("TRACK_SPEED")

    # 업로드
    df_speed1l["DEVICE_KEY", "MDT_ID", "VH_ID", "BUS_NO", "BUS_COMPANY", "LATITUDE", "LONGITUDE", "ANGLE", \
                "LINK_ID", "SPEED", "ROUTE_ID", "ROUTE_NO", "MAX_SPD", "COLLECTION_DATE"].withColumn('WORK_DATE', F.lit(datetime.today().date())).write.mode("append").insertInto("MD_SPEED")
