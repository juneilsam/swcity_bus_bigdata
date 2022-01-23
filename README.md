# swcity_bus_bigdata

## 수원시 광역(시외) 버스 스마트 안전 서비스 구축

node_link_buff.py : 도로 링크의 string 좌표 형식을 polygon으로 변환

over_speed_100.py : 시속 100이 넘는 버스만을 판별

over_speed_poly.py : 초당 데이터를 기준으로 당시의 도로 최대 속도를 기준으로 과속 버스를 판별

failed_speed.py : 과속 버스 판별에 실패한 코드 모음

21년 10월 ~ ‘22년 1월(4개월)

분석
- 로그 데이터 분류/적재 코드 구성
- 로그 데이터 분류
- 분류 데이터 적재

임시 데이터 생성
- 실데이터 수집 완료 전 작업 준비에 사용될 데이터 생성

알고리즘 선정
- 최종 데이터 분류에 사용될 알고리즘 선정

- 수원시 버스 주행기록 데이터
- 서울/경기/인천 도로 공공 데이터

언어, 기술 – Python, PySpark, Spark SQL, Sedona, GeoPandas, Koalas

Tool - Docker, Hadoop, Hive, NiFi, Jupyter Notebook, Zeppelin, VSCode, Putty, Maria DB

알고리즘 – Random Forest

OS - Window, Linux
