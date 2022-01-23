# 필요한 패키지, 라이브러리
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F

import sedona
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.register import SedonaRegistrator

from datetime import datetime
from datetime import timedelta

# 스파크 사용을 위한 세션 빌드
spark = SparkSession.builder.appName('spark'). \
                            config("spark.serializer", KryoSerializer.getName). \
                            config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
                            config('spark.jars.packages',
                                'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,org.datasyslab:geotools-wrapper:1.1.0-24.0') \
                            .enableHiveSupport().getOrCreate()

# Sedona 사용을 위한 register
SedonaRegistrator.registerAll(spark)

# 어제 데이터만 불러오기 위함
yesterday = datetime.today().date() - timedelta(days=1)

# spark sql을 사용하여 원본 로그 데이터에서 필요한 컬럼 불러오기
gnss_sql = spark.sql(f"SELECT gnssDF.DEVICE_KEY, gnssDF.MDT_ID, gnssDF.BUS_NO, gnssDF.BUS_ROUTE_ID as ROUTE_ID, gnssDF.VH_ID, gnssDF.BUS_COMPANY, gnssDF.BUS_ROUTE_NO as ROUTE_NO, gnssDF.COLLECTION_DATE, \
    gnssDF.SPEED, gnssDF.LONGITUDE, gnssDF.LATITUDE, gnssDF.ANGLE, ST_POINT(LONGITUDE, LATITUDE) AS geom FROM dp_gnss as gnssDF where partition='{yesterday}' and bus_route_id != 'null'")

# 도로 링크 데이터에서 필요한 컬럼 불러오기
node_sql = spark.sql("SELECT LINK_ID, MAX_SPD, ST_PolygonFromText(geometry, ',') as geometry from row_link")

# 파티션을 나누기 위해 사용되는 기준 1. 장치 키 2. 제한 속도
mac = gnss_sql.select('DEVICE_KEY').distinct().collect()
spd_collect = node_sql.select('MAX_SPD').distinct().collect()

print("start :", datetime.now())
for i in mac:
    start = datetime.now()
    device = i[0]
    
    # 데이터 분할
    df_speed1 = gnss_sql.filter(gnss_sql.DEVICE_KEY == device)
    
    # 최대 시속이 같은 도로 링크 아이디만 불러온다
    for j in spd_collect:
        max_spd = j[0]
        node_df  = node_sql.filter(node_sql.MAX_SPD == max_spd)
        df_speed1.createOrReplaceTempView('df_speed')
        node_df.createOrReplaceTempView('node_df1')
        
        # buffer로 확장된 폴리곤에 해당되는 기록만 판별
        temp_df1 = spark.sql("select DEVICE_KEY, MDT_ID, VH_ID, BUS_NO, BUS_COMPANY, LATITUDE, LONGITUDE, ANGLE, \
            LINK_ID, SPEED, ROUTE_NO, node_df1.MAX_SPD, COLLECTION_DATE, ROUTE_ID from node_df1, df_speed WHERE ST_Within(df_speed.geom, node_df1.geometry)")
            
        # 과속 데이터인지 판별
        temp_df1 = temp_df1.filter(temp_df1.SPEED > (temp_df1.MAX_SPD + 20))
        
        # 메모리 확보를 위한 임시 데이터 삭제
        del node_df
        
        # 다른 테이블에 필요한 컬럼 추가
        temp_df2 = temp_df1["DEVICE_KEY", "MDT_ID", "VH_ID", "BUS_NO", "BUS_COMPANY", "LATITUDE", "LONGITUDE", "ANGLE", \
            "LINK_ID", "SPEED", "ROUTE_ID", "ROUTE_NO", "MAX_SPD", "COLLECTION_DATE"].withColumn('WORK_DATE', F.lit(datetime.today().date()))

        # 테이블에 데이터 업로드
        temp_df1.write.mode("append").insertInto("TRACK_SPEED")
        temp_df2.write.mode("append").insertInto("MD_SPEED")

        # 임시 테이블 삭제
        del temp_df1
        del temp_df2
    finish = datetime.now()
    print(finish - start)
print("finish :", datetime.now())
