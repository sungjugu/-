############################################################
# 파일명  preprocessing.py
# 작성    2023-03-15
# 1차수정 2023-03-16
# 작성자  구성주
############################################################

######################
# 0. DIRECTORY
######################
model_dir = 'C:/Users/sungj/python/Batch2/Model'
data_dir = 'C:/Users/sungj/python/Batch2/Data'
log_dir = 'C:/Users/sungj/python/Batch2/Log'
db_dir = 'C:/Users/sungj/python/pine.db'


################################
# 1. import
################################
import sys
from sklearn.preprocessing import OneHotEncoder
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
# 2. 파라미터(기준년월) 받기, 기준년월 설정
################################
yearmonth = sys.argv[1]
start = datetime.now()
dt = datetime.strptime(yearmonth, '%Y%m')
dt_minus_one_month = dt - relativedelta(months=1)
six_months_ago = dt - relativedelta(months=6)
one_year_later = dt - relativedelta(years=1)
dt_minus_one_month = dt_minus_one_month.strftime('%Y%m')
ym_six_bf = six_months_ago.strftime("%Y%m")
ym_one_bf = one_year_later.strftime("%Y%m") 

print(f'[LOG] [{str(start).split(".")[0]}] preprocessing - 프로그램 시작 - 기준년월 {dt_minus_one_month}')


################################
# 3. DB 연결
################################
st_tm_tot = datetime.now()
print(f'[LOG] [{str(st_tm_tot).split(".")[0]}] preprocessing - STEP01 DB 연결 시작]')

# 일반연결
conn = sqlite3.connect(f"{db_dir}", isolation_level=None)
cur = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2 = engine.connect()

ed_tm_tot = datetime.now() 
el_tm_tot = ed_tm_tot - st_tm_tot 
print(f'[LOG] [{str(ed_tm_tot).split(".")[0]}] preprocessing - STEP01 DB 연결 완료]')
print(f'[LOG] [DB연결 소요시간 : {str(el_tm_tot).split(".")[0]}]')


################################
# 2. 마트 불러오기
################################
mart_start_tot = datetime.now()
print(f'[LOG] [{str(mart_start_tot).split(".")[0]}] preprocessing - STEP02 마트 호출 시작]')

mart = pd.read_sql(
    f"""
    select 
          고객ID                 
        , 기준년월               
        , history_영화평가       
        , history_영화시청완료   
        , history_영화시청시작   
        , history_영화검색       
        , history_영화구매       
        , animation_영화평가     
        , animation_영화시청완료 
        , animation_영화시청시작 
        , animation_영화검색     
        , animation_영화구매     
        , drama_영화평가         
        , drama_영화시청완료     
        , drama_영화시청시작     
        , drama_영화검색         
        , drama_영화구매         
        , comedy_영화평가        
        , comedy_영화시청완료    
        , comedy_영화시청시작    
        , comedy_영화검색        
        , comedy_영화구매        
        , action_영화평가        
        , action_영화시청완료    
        , action_영화시청시작    
        , action_영화검색        
        , action_영화구매        
        , crime_영화평가         
        , crime_영화시청완료     
        , crime_영화시청시작     
        , crime_영화검색         
        , crime_영화구매         
        , thriller_영화평가      
        , thriller_영화시청완료  
        , thriller_영화시청시작  
        , thriller_영화검색      
        , thriller_영화구매      
        , documentary_영화평가   
        , documentary_영화시청완료
        , documentary_영화시청시작
        , documentary_영화검색   
        , documentary_영화구매   
        , adventure_영화평가     
        , adventure_영화시청완료 
        , adventure_영화시청시작 
        , adventure_영화검색     
        , adventure_영화구매     
        , fantasy_영화평가       
        , fantasy_영화시청완료   
        , fantasy_영화시청시작   
        , fantasy_영화검색       
        , fantasy_영화구매       
        , family_영화평가        
        , family_영화시청완료    
        , family_영화시청시작    
        , family_영화검색        
        , family_영화구매        
        , romance_영화평가       
        , romance_영화시청완료   
        , romance_영화시청시작   
        , romance_영화검색       
        , romance_영화구매       
        , music_영화평가         
        , music_영화시청완료     
        , music_영화시청시작     
        , music_영화검색         
        , music_영화구매         
        , horror_영화평가        
        , horror_영화시청완료    
        , horror_영화시청시작    
        , horror_영화검색        
        , horror_영화구매        
        , war_영화평가           
        , war_영화시청완료       
        , war_영화시청시작       
        , war_영화검색           
        , war_영화구매           
        , western_영화평가       
        , western_영화시청완료   
        , western_영화시청시작   
        , western_영화검색       
        , western_영화구매       
        , mystery_영화평가       
        , mystery_영화시청완료   
        , mystery_영화시청시작   
        , mystery_영화검색       
        , mystery_영화구매       
        , short_영화평가         
        , short_영화시청완료     
        , short_영화시청시작     
        , short_영화검색         
        , short_영화구매         
        , musical_영화평가       
        , musical_영화시청완료   
        , musical_영화시청시작   
        , musical_영화검색       
        , musical_영화구매       
        , sport_영화평가         
        , sport_영화시청완료     
        , sport_영화시청시작     
        , sport_영화검색         
        , sport_영화구매         
        , scifi_영화평가         
        , scifi_영화시청완료     
        , scifi_영화시청시작     
        , scifi_영화검색         
        , scifi_영화구매         
        , biography_영화평가     
        , biography_영화시청완료 
        , biography_영화시청시작 
        , biography_영화검색     
        , biography_영화구매     
        , news_영화평가          
        , news_영화시청완료      
        , news_영화시청시작      
        , news_영화검색          
        , news_영화구매          
        , 나라ID                 
        , 나이                   
        , 연령대                 
        , 대륙ID                 
        , 통근거리               
        , 교육                   
        , 성별                   
        , 가구규모               
        , 직업종류               
        , 결혼여부               
        , 애완동물               
        , 고객이_된_년수     
        , six_month_active
        , one_year_active
    from 피처마트2
    where 기준년월 = {dt_minus_one_month}
        and one_year_active = '1'
    """
, conn        
) 
mart_end_tot = datetime.now() 
mart_tot = mart_end_tot - mart_start_tot 
print(f'[LOG] [{str(mart_end_tot).split(".")[0]}] preprocessing - STEP02 마트 호출 완료]')
print(f'[LOG] [마트 호출 소요시간 : {str(mart_tot).split(".")[0]}]')
   
   
################################
# 2. 데이터 전처리
################################  
preprocessing_start_tot = datetime.now()
print(f'[LOG] [{str(preprocessing_start_tot).split(".")[0]}] preprocessing - STEP03 전처리 시작]')

mart = mart.drop(['나이', 'six_month_active', 'one_year_active'], axis=1)
mart.loc[mart['통근거리'] == -1, '통근거리'] = 0

# 원핫인코더 호출
with open('C:/Users/sungj/python/Batch/Data/onehot_encoder.pickle', 'rb') as f:
    encoder = pickle.load(f)

columns_to_encode = ['연령대', '나라ID', '대륙ID', '교육', '직업종류', '애완동물', '결혼여부', '성별']
encoded_data = encoder.transform(mart[columns_to_encode])
encoded_df = pd.DataFrame(encoded_data, columns=encoder.get_feature_names_out(columns_to_encode))

mart.drop(columns_to_encode, axis=1, inplace=True)
mart = pd.concat([mart, encoded_df], axis=1)

# 변수 지정
X = mart.drop(['기준년월', '고객ID'], axis=1)

preprocessing_end_tot = datetime.now() 
preprocessing_tot = preprocessing_end_tot - preprocessing_start_tot 
print(f'[LOG] [{str(preprocessing_end_tot).split(".")[0]}] preprocessing - STEP03 전처리 완료]')
print(f'[LOG] [전처리 소요시간 : {str(preprocessing_tot).split(".")[0]}]')


################################
# 3. 데이터 저장
################################  
pickle_start_tot = datetime.now()
print(f'[LOG] [{str(pickle_start_tot).split(".")[0]}] preprocessing - STEP04 데이터 저장 시작]')

# pickle 파일로 저장
with open(f'{data_dir}/mart.pickle', 'wb') as f:
    pickle.dump(mart, f)
    
# pickle 파일로 저장
with open(f'{data_dir}/X.pickle', 'wb') as f:
    pickle.dump(X, f)
    
pickle_end_tot = datetime.now() 
pickle_tot = pickle_end_tot - pickle_start_tot 
print(f'[LOG] [{str(pickle_end_tot).split(".")[0]}] preprocessing - STEP04 데이터 저장 완료]')
print(f'[LOG] [데이터 저장 소요시간 : {str(pickle_tot).split(".")[0]}]')

end = datetime.now() 
all_time = end - start 
print(f'[LOG] [{str(end).split(".")[0]}] preprocessing - 프로그램 완료 - 기준년월 {dt_minus_one_month}')
print(f'[LOG] [프로그램 소요시간 : {str(all_time).split(".")[0]}]')