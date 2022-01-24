# koalas 데이터 프레임을 이용함
node_k = node_.to_koalas()
for i in range(len(device_key)):
    print(i)
    device = device_key[i][0]
    gnss_sql = spark.sql(f"SELECT \
                        low_gnss.DEVICE_KEY,\
                        low_gnss.MDT_ID,\
                        low_gnss.BUS_ROUTE_ID as ROUTE_ID,\
                        low_gnss.BUS_VH_ID,\
                        low_gnss.BUS_COMPANY,\
                        low_gnss.COLLECTION_DATE AS  COLLECT_DATE ,\
                        low_gnss.SPEED,\
                        low_gnss.LONGITUDE,\
                        low_gnss.LATITUDE, \
                        ST_POINT(LONGITUDE,LATITUDE) AS geom \
                        FROM    low_gnss WHERE  DEVICE_KEY = '{device}'")
    
    for link in set(node_k.LINK_ID.to_list()):
        temp = node_.filter(F.col("LINK_ID") == f'{link}')
        temp.createOrReplaceTempView('temp')
        gnss_sql.createOrReplaceTempView('df_speed')
        temp_df = spark.sql("select ST_DISTANCE(ST_POINT(LONGITUDE, LATITUDE), temp.geometry) as geom from temp, df_speed").to_koalas()
        temp_df[temp_df.geom == temp_df.geom.describe().min()]
    break
    
