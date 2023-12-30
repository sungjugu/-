############################################################
# 파일명  model.py
# 작성    2023-03-15
# 1차수정 2023-03-16
# 작성자  구성주
############################################################

######################
# 0. DIRECTORY
######################
model_dir = 'C:/Users/sungj/python/Batch/Model'
data_dir = 'C:/Users/sungj/python/Batch/Data'
log_dir = 'C:/Users/sungj/python/Batch/Log'
db_dir = 'C:/Users/sungj/python/pine.db'


################################
# 1. import
################################
import sys
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
import xgboost as xgb
import pandas as pd
import numpy as np
import sqlite3
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pickle


################################
# 2. 파라미터(기준년월) 받기
################################
yearmonth = sys.argv[1]
start = datetime.now()
dt = datetime.strptime(yearmonth, '%Y%m')
dt_minus_one_month = dt - relativedelta(months=1)
dt_minus_one_month = dt_minus_one_month.strftime('%Y%m')

print(f'[LOG] [{str(start).split(".")[0]}] model - 프로그램 시작 - 기준년월 {dt_minus_one_month}')


################################
# 3. DB 연결
################################
st_tm_tot = datetime.now()
print(f'[LOG] [{str(st_tm_tot).split(".")[0]}] model - STEP01 DB 연결 시작]')
conn = sqlite3.connect(f"{db_dir}", isolation_level=None)
cur = conn.cursor()

engine = create_engine('sqlite:///pine.db')
conn2 = engine.connect()

ed_tm_tot = datetime.now() 
el_tm_tot = ed_tm_tot - st_tm_tot 
print(f'[LOG] [{str(ed_tm_tot).split(".")[0]}] model - STEP01 DB 연결 완료]')
print(f'[LOG] [DB연결 소요시간 : {str(el_tm_tot).split(".")[0]}]')


################################
# 4. 데이터 & 모델 & 마트 호출
################################
data_start_tot = datetime.now()
print(f'[LOG] [{str(data_start_tot).split(".")[0]}] model - STEP02 데이터 호출 시작]')

with open(f'{data_dir}/X.pickle', 'rb') as f:
    X = pickle.load(f)
    
with open(f'{data_dir}/random_forest_model.pkl', 'rb') as f:
    rf_model = pickle.load(f)
    
with open(f'{data_dir}/mart.pickle', 'rb') as f:
    mart = pickle.load(f)
    
data_end_tot = datetime.now() 
data_tot = data_end_tot - data_start_tot 
print(f'[LOG] [{str(data_end_tot).split(".")[0]}] model - STEP02 데이터 호출 완료]')
print(f'[LOG] [데이터 호출 소요시간 : {str(data_tot).split(".")[0]}]')
    
    
################################
# 5. RF 예측
################################
model_start_tot = datetime.now()
print(f'[LOG] [{str(model_start_tot).split(".")[0]}] model - STEP03 모델 예측 시작]')

cutoff = 0.39
y_pred_prob = rf_model.predict_proba(X)[:, 1]
y_pred_binary = [1 if x > cutoff else 0 for x in y_pred_prob]
prediction = pd.Series(y_pred_binary, name='prediction')

# 결과 데이터프레임 만들기
result = pd.DataFrame({'prediction': y_pred_prob})
result['기준년월'] = f'{yearmonth}'
result['고객ID'] = mart['고객ID']
result['확률'] = result['prediction'].apply(lambda x: round(x, 2))

result = result.drop('prediction', axis=1)
result['장르'] = 'sci-fi'
result = result[['기준년월', '고객ID', '장르', '확률']]

model_end_tot = datetime.now() 
model_tot = model_end_tot - model_start_tot 
print(f'[LOG] [{str(model_end_tot).split(".")[0]}] model - STEP03 모델 예측 완료]')
print(f'[LOG] [모델 예측 소요시간 : {str(model_tot).split(".")[0]}]')

################################
# 3. 결과적재
################################
insert_start_tot = datetime.now()
print(f'[LOG] [{str(insert_start_tot).split(".")[0]}] model - STEP04 결과 적재 시작]')

cur.execute(
    f"""
    delete
    from result
    where 기준년월 = {yearmonth}
    """
)

try:
    st_tm_tot = datetime.now()
    result.to_sql(
          name = 'result'
        , con = conn
        , if_exists='append'
        , index = False
        , method = 'multi'
        , chunksize = 10000
    )
    ed_tm_tot = datetime.now() 
    el_tm_tot = ed_tm_tot - st_tm_tot 
    print("[LOG] 적재성공")
except:
    print("에러")


DB건수 = pd.read_sql(
    """
        select count(1) as 건수
        from result
    """
, conn
).values[0][0]


insert_end_tot = datetime.now() 
insert_tot = insert_end_tot - insert_start_tot 
print(f'[LOG] [{str(insert_end_tot).split(".")[0]}] model - STEP04 결과 적재 완료]')
print(f'[LOG] [결과 적재 소요시간 : {str(insert_tot).split(".")[0]}]')
print(rf"[LOG] [적재 건수 = {DB건수:,}]")

end = datetime.now() 
all_time = end - start 
print(f'[LOG] [{str(end).split(".")[0]}] mart - 프로그램 완료 - 기준년월 {dt_minus_one_month}')
print(f'[LOG] [프로그램 소요시간 : {str(all_time).split(".")[0]}]')