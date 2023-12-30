################################
# 0. import 및 DB연결
################################
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

# 일반연결
conn = sqlite3.connect("pine.db", isolation_level=None)
cur = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2 = engine.connect()



################################
# 1. 데이터 불러오기
################################
df_test = pd.read_sql(
                """
            select 
                  history_영화평가
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
                , case
                    when 나이 < 20 then '10대'
                    when 나이 < 30 then '20대'
                    when 나이 < 40 then '30대'
                    when 나이 < 50 then '40대'
                    when 나이 < 60 then '50대'
                    when 나이 < 70 then '60대'
                    when 나이 < 80 then '70대'
                    when 나이 < 90 then '80대'
                    else 나이
                  end as 연령대
                , 대륙ID
                , 통근거리
                , 교육
                , 성별
                , 가구규모
                , 직업종류
                , 결혼여부
                , 애완동물
                , 고객이_된_년수
                , 이벤트여부
            from test_data_set3
               """
, conn
)

# 통근거리 -1값 처리
df_test.loc[df_test['통근거리'] == -1, '통근거리'] = 0

# 원핫인코더 호출
with open('onehot_encoder.pickle', 'rb') as f:
    encoder = pickle.load(f)

columns_to_encode = ['연령대', '나라ID', '대륙ID', '교육', '직업종류', '애완동물', '결혼여부', '성별']
encoded_data = encoder.transform(df_test[columns_to_encode])
encoded_df = pd.DataFrame(encoded_data, columns=encoder.get_feature_names_out(columns_to_encode))

df_test.drop(columns_to_encode, axis=1, inplace=True)
df_test = pd.concat([df_test, encoded_df], axis=1)


# 출력범위 조정
# pd.set_option('display.max_seq_items', None)
# pd.set_option('display.width', 10)

# null값 확인
df_test.isnull().sum()

# 변수 지정
X = df_test.drop('이벤트여부', axis=1)
Y = df_test['이벤트여부']
Y = np.array(Y)
Y = Y.reshape(-1, 1)
y_valid_int = Y.astype(int)


################################
# 2. RF 예측
################################
with open('random_forest_model.pkl', 'rb') as f:
    rf_model = pickle.load(f)

cutoff = 0.37
y_pred_prob = rf_model.predict_proba(X)[:, 1]
y_pred_binary = [1 if x > cutoff else 0 for x in y_pred_prob]
prediction = pd.Series(y_pred_binary, name='prediction')
report = classification_report(y_valid_int, y_pred_binary)
print(report)