################################
# 0. import 및 DB연결
################################
import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# 일반연결
conn = sqlite3.connect("pine.db", isolation_level=None)
cur = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2 = engine.connect()


################################
# 1. 데이터 가져오기
################################
df = pd.read_sql(
                """
                    select 
                          a.고객ID     
                        , a.기준년월   
                        , a.scifi     
                        , b.성별
                        , b.연령대
                    from 타겟마트2 a
                    left join (
                        select 
                             고객ID
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
                           , 성별
                        from CUSTOMER
                    ) b
                    on 
                            a.고객ID = b.고객ID
                    where a.six_month_active = '1'
                    
               """
, conn
)
df

print('-----------------------------------')
print('데이터 가져오기 완료')
# print(df)
print('-----------------------------------')


################################
# 2. 테이블 만들기
################################
# 데이터 delete
cur.execute(
    f"""
    drop table if exists 타겟마트2_sample
    """
)

# 테이블 create
cur.execute(
    f"""
    create table if not exists 타겟마트2_sample(
      기준년월
    , 고객ID
    , 장르
    , 이벤트여부
    , 샘플링배수
    )
    """
)


################################
# 3. 1배수-3배수 샘플링 후 적재
################################
all_sample_df = pd.DataFrame()

baseym = ['201208', '201207', '201206', '201205', '201204', '201203', '201202', '201201',
          '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101',
          '201012', '201011', '201010', '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001']
for i in range(1, 4):
    all_dfs = pd.DataFrame()
    
    for ym in baseym:
        
        df_0 = df[(df['scifi'] == '0') &  (df['기준년월'] == ym)]
        df_1 = df[(df['scifi'] == '1') &  (df['기준년월'] == ym)]
        
        event_len = len(df_1)
        nonevent_len = len(df_0)
        
        # 성별, 연령대 건수
        sex_age_count = pd.DataFrame(df_0.groupby(['성별', '연령대'])['고객ID'].count())
        
        # 성별, 연령대 비율
        sex_age_per = sex_age_count/nonevent_len
        
        # value만 추출
        sex_age_per_values = sex_age_per['고객ID'].values
        
        # index
        sex_age_per_index = sex_age_per.index
        
        for j in range(len(sex_age_per_index)) :
            
            # j번째 값 추출
            idx = sex_age_per_index[j]
            
            # 추출 할 개수  
            count = round(sex_age_per_values[j]*event_len*i)
            
            df_0_temp = df_0.loc[(df_0['성별']==idx[0]) & (df_0['연령대']==idx[1])]
            
            df_sample = df_0_temp.sample(n=count, random_state=1004 ,replace = False)
            
            all_dfs = pd.concat([all_dfs, df_sample], ignore_index=True)
        
        all_dfs = pd.concat([all_dfs, df_1], ignore_index=True)
        
    all_dfs.drop(['연령대','성별'], axis=1, inplace=True)
    all_dfs = all_dfs.rename(columns={'scifi': '이벤트여부'})
    all_dfs['샘플링배수'] = i
    all_dfs['장르'] = 'scifi'
    
    # 데이터프레임 컬럼 순서 변경
    all_dfs = all_dfs.reindex(columns=['기준년월', '고객ID', '장르', '이벤트여부', '샘플링배수'])

    # 적재 전 삭제
    cur.execute(
            f"""
                delete
                from 타겟마트2_sample
                where 
                    기준년월 = {ym}
                and 샘플링배수 = {i}
            """
     )
        
    # 적재
    st_tm_tot = datetime.now()
    all_dfs.to_sql(
        name = '타겟마트2_sample'
        , con = engine
        , if_exists='append'
        , index = False
        , method = 'multi'
        , chunksize = 10000
    )
    ed_tm_tot = datetime.now() 
    el_tm_tot = ed_tm_tot - st_tm_tot 
    
    print(f'{i}번째 적재 소요시간 = {str(el_tm_tot).split(".")[0]}')


# DB건수 확인
DB건수 = pd.read_sql(
    """
        select count(1) as 건수
        from 타겟마트2_sample
    """
, conn
).values[0][0]

print(rf"DB건수 = {DB건수:,}")


################################
# 4. train 마트 생성 (train_data_set2)
################################
cur.execute(
    f"""
    drop table if exists train_data_set2
    """
)

baseym = ['201207', '201206', '201205', '201204', '201203', '201202', '201201',
          '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101',
          '201012', '201011', '201010', '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001']
          

cur.execute(
        f"""
        create table if not exists train_data_set2(
          고객ID                        text
        , 기준년월                      INT
        , history_영화평가              INT
        , history_영화시청완료          INT
        , history_영화시청시작          INT
        , history_영화검색              INT
        , history_영화구매              INT
        , animation_영화평가            INT
        , animation_영화시청완료        INT
        , animation_영화시청시작        INT
        , animation_영화검색            INT
        , animation_영화구매            INT
        , drama_영화평가                INT
        , drama_영화시청완료            INT
        , drama_영화시청시작            INT
        , drama_영화검색                INT
        , drama_영화구매                INT
        , comedy_영화평가               INT
        , comedy_영화시청완료           INT
        , comedy_영화시청시작           INT
        , comedy_영화검색               INT
        , comedy_영화구매               INT
        , action_영화평가               INT
        , action_영화시청완료           INT
        , action_영화시청시작           INT
        , action_영화검색               INT
        , action_영화구매               INT
        , crime_영화평가                INT
        , crime_영화시청완료            INT
        , crime_영화시청시작            INT
        , crime_영화검색                INT
        , crime_영화구매                INT
        , thriller_영화평가             INT
        , thriller_영화시청완료         INT
        , thriller_영화시청시작         INT
        , thriller_영화검색             INT
        , thriller_영화구매             INT
        , documentary_영화평가          INT
        , documentary_영화시청완료      INT
        , documentary_영화시청시작      INT
        , documentary_영화검색          INT
        , documentary_영화구매          INT
        , adventure_영화평가            INT
        , adventure_영화시청완료        INT
        , adventure_영화시청시작        INT
        , adventure_영화검색            INT
        , adventure_영화구매            INT
        , fantasy_영화평가              INT
        , fantasy_영화시청완료          INT
        , fantasy_영화시청시작          INT
        , fantasy_영화검색              INT
        , fantasy_영화구매              INT
        , family_영화평가               INT
        , family_영화시청완료           INT
        , family_영화시청시작           INT
        , family_영화검색               INT
        , family_영화구매               INT
        , romance_영화평가              INT
        , romance_영화시청완료          INT
        , romance_영화시청시작          INT
        , romance_영화검색              INT
        , romance_영화구매              INT
        , music_영화평가                INT
        , music_영화시청완료            INT
        , music_영화시청시작            INT
        , music_영화검색                INT
        , music_영화구매                INT
        , horror_영화평가               INT
        , horror_영화시청완료           INT
        , horror_영화시청시작           INT
        , horror_영화검색               INT
        , horror_영화구매               INT
        , war_영화평가                  INT
        , war_영화시청완료              INT
        , war_영화시청시작              INT
        , war_영화검색                  INT
        , war_영화구매                  INT
        , western_영화평가              INT
        , western_영화시청완료          INT
        , western_영화시청시작          INT
        , western_영화검색              INT
        , western_영화구매              INT
        , mystery_영화평가              INT
        , mystery_영화시청완료          INT
        , mystery_영화시청시작          INT
        , mystery_영화검색              INT
        , mystery_영화구매              INT
        , short_영화평가                INT
        , short_영화시청완료            INT
        , short_영화시청시작            INT
        , short_영화검색                INT
        , short_영화구매                INT
        , musical_영화평가              INT
        , musical_영화시청완료          INT
        , musical_영화시청시작          INT
        , musical_영화검색              INT
        , musical_영화구매              INT
        , sport_영화평가                INT
        , sport_영화시청완료            INT
        , sport_영화시청시작            INT
        , sport_영화검색                INT
        , sport_영화구매                INT
        , scifi_영화평가                INT
        , scifi_영화시청완료            INT
        , scifi_영화시청시작            INT
        , scifi_영화검색                INT
        , scifi_영화구매                INT
        , biography_영화평가            INT
        , biography_영화시청완료        INT
        , biography_영화시청시작        INT
        , biography_영화검색            INT
        , biography_영화구매            INT
        , news_영화평가                 INT
        , news_영화시청완료             INT
        , news_영화시청시작             INT
        , news_영화검색                 INT
        , news_영화구매                 INT
        , 나라ID                        INT
        , 나이						    INT
        , 대륙ID						text
        , 통근거리					    INT
        , 교육						    text
        , 성별						    text
        , 가구규모					    INT
        , 직업종류					    text
        , 결혼여부					    text
        , 애완동물					    text
        , 고객이_된_년수				INT
        , 이벤트여부                    INT 
        )                          
        """
)
         
datetime_format = "%Y%m"
for ym in baseym :
    formatted_date_현재 = ym
    
    before_base_list = datetime.strptime(ym, datetime_format)
    one_months_after = before_base_list + relativedelta(months = 1)
    ym_af = one_months_after.strftime("%Y%m") 

    cur.execute(
        f"""
        INSERT INTO train_data_set2
        select 
              a1.고객ID
            , a1.기준년월
            , a1.history_영화평가
            , a1.history_영화시청완료
            , a1.history_영화시청시작
            , a1.history_영화검색
            , a1.history_영화구매
            , a1.animation_영화평가
            , a1.animation_영화시청완료
            , a1.animation_영화시청시작
            , a1.animation_영화검색
            , a1.animation_영화구매
            , a1.drama_영화평가
            , a1.drama_영화시청완료
            , a1.drama_영화시청시작
            , a1.drama_영화검색
            , a1.drama_영화구매
            , a1.comedy_영화평가
            , a1.comedy_영화시청완료
            , a1.comedy_영화시청시작
            , a1.comedy_영화검색
            , a1.comedy_영화구매
            , a1.action_영화평가
            , a1.action_영화시청완료
            , a1.action_영화시청시작
            , a1.action_영화검색
            , a1.action_영화구매
            , a1.crime_영화평가
            , a1.crime_영화시청완료
            , a1.crime_영화시청시작
            , a1.crime_영화검색
            , a1.crime_영화구매
            , a1.thriller_영화평가
            , a1.thriller_영화시청완료
            , a1.thriller_영화시청시작
            , a1.thriller_영화검색
            , a1.thriller_영화구매
            , a1.documentary_영화평가
            , a1.documentary_영화시청완료
            , a1.documentary_영화시청시작
            , a1.documentary_영화검색
            , a1.documentary_영화구매
            , a1.adventure_영화평가
            , a1.adventure_영화시청완료
            , a1.adventure_영화시청시작
            , a1.adventure_영화검색
            , a1.adventure_영화구매
            , a1.fantasy_영화평가
            , a1.fantasy_영화시청완료
            , a1.fantasy_영화시청시작
            , a1.fantasy_영화검색
            , a1.fantasy_영화구매
            , a1.family_영화평가
            , a1.family_영화시청완료
            , a1.family_영화시청시작
            , a1.family_영화검색
            , a1.family_영화구매
            , a1.romance_영화평가
            , a1.romance_영화시청완료
            , a1.romance_영화시청시작
            , a1.romance_영화검색
            , a1.romance_영화구매
            , a1.music_영화평가
            , a1.music_영화시청완료
            , a1.music_영화시청시작
            , a1.music_영화검색
            , a1.music_영화구매
            , a1.horror_영화평가
            , a1.horror_영화시청완료
            , a1.horror_영화시청시작
            , a1.horror_영화검색
            , a1.horror_영화구매
            , a1.war_영화평가
            , a1.war_영화시청완료
            , a1.war_영화시청시작
            , a1.war_영화검색
            , a1.war_영화구매
            , a1.western_영화평가
            , a1.western_영화시청완료
            , a1.western_영화시청시작
            , a1.western_영화검색
            , a1.western_영화구매
            , a1.mystery_영화평가
            , a1.mystery_영화시청완료
            , a1.mystery_영화시청시작
            , a1.mystery_영화검색
            , a1.mystery_영화구매
            , a1.short_영화평가
            , a1.short_영화시청완료
            , a1.short_영화시청시작
            , a1.short_영화검색
            , a1.short_영화구매
            , a1.musical_영화평가
            , a1.musical_영화시청완료
            , a1.musical_영화시청시작
            , a1.musical_영화검색
            , a1.musical_영화구매
            , a1.sport_영화평가
            , a1.sport_영화시청완료
            , a1.sport_영화시청시작
            , a1.sport_영화검색
            , a1.sport_영화구매
            , a1.scifi_영화평가
            , a1.scifi_영화시청완료
            , a1.scifi_영화시청시작
            , a1.scifi_영화검색
            , a1.scifi_영화구매
            , a1.biography_영화평가
            , a1.biography_영화시청완료
            , a1.biography_영화시청시작
            , a1.biography_영화검색
            , a1.biography_영화구매
            , a1.news_영화평가
            , a1.news_영화시청완료
            , a1.news_영화시청시작
            , a1.news_영화검색
            , a1.news_영화구매
            , a1.나라ID
            , a1.나이
            , a1.대륙ID
            , a1.통근거리
            , a1.교육
            , a1.성별
            , a1.가구규모
            , a1.직업종류
            , a1.결혼여부
            , a1.애완동물
            , a1.고객이_된_년수
            , a2.이벤트여부
        from (
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
                 , 대륙ID
                 , 통근거리
                 , 교육
                 , 성별
                 , 가구규모
                 , 직업종류
                 , 결혼여부
                 , 애완동물
                 , 고객이_된_년수
            from 피처마트
            where 기준년월 = '{ym}'
        ) a1 
        inner join (
            select 
                  고객ID
                , 기준년월
                , 이벤트여부
                , 샘플링배수
                , 장르
            from 타겟마트2_sample
            where 기준년월 = '{ym_af}'
                and 샘플링배수 = 3
                and 장르 = 'scifi'
        ) a2
        on a1.고객ID = a2.고객ID
        """
    )
    
    cnt_1 = pd.read_sql(
        f"""
        select count(*)       as cnt_1
        from train_data_set2
        where 기준년월 = '{ym}'
        """
    ,conn
    )
    
    # print('insert 확인')
            
            
    print(f"{ym} > insert : {cnt_1['cnt_1'][0]}건 확인")
    
DB건수 = pd.read_sql(
    """
    select count(1) as 건수
    from train_data_set2
    """
, conn
).values[0][0]

print(rf"[LOG] DB건수 = {DB건수:,}")


################################
# 5. test 마트 생성 (test_data_set2)
################################
cur.execute(
    f"""
    drop table if exists test_data_set2
    """
)

cur.execute(
        f"""
        create table if not exists test_data_set2(
          고객ID                        text
        , 기준년월                      INT
        , history_영화평가              INT
        , history_영화시청완료          INT
        , history_영화시청시작          INT
        , history_영화검색              INT
        , history_영화구매              INT
        , animation_영화평가            INT
        , animation_영화시청완료        INT
        , animation_영화시청시작        INT
        , animation_영화검색            INT
        , animation_영화구매            INT
        , drama_영화평가                INT
        , drama_영화시청완료            INT
        , drama_영화시청시작            INT
        , drama_영화검색                INT
        , drama_영화구매                INT
        , comedy_영화평가               INT
        , comedy_영화시청완료           INT
        , comedy_영화시청시작           INT
        , comedy_영화검색               INT
        , comedy_영화구매               INT
        , action_영화평가               INT
        , action_영화시청완료           INT
        , action_영화시청시작           INT
        , action_영화검색               INT
        , action_영화구매               INT
        , crime_영화평가                INT
        , crime_영화시청완료            INT
        , crime_영화시청시작            INT
        , crime_영화검색                INT
        , crime_영화구매                INT
        , thriller_영화평가             INT
        , thriller_영화시청완료         INT
        , thriller_영화시청시작         INT
        , thriller_영화검색             INT
        , thriller_영화구매             INT
        , documentary_영화평가          INT
        , documentary_영화시청완료      INT
        , documentary_영화시청시작      INT
        , documentary_영화검색          INT
        , documentary_영화구매          INT
        , adventure_영화평가            INT
        , adventure_영화시청완료        INT
        , adventure_영화시청시작        INT
        , adventure_영화검색            INT
        , adventure_영화구매            INT
        , fantasy_영화평가              INT
        , fantasy_영화시청완료          INT
        , fantasy_영화시청시작          INT
        , fantasy_영화검색              INT
        , fantasy_영화구매              INT
        , family_영화평가               INT
        , family_영화시청완료           INT
        , family_영화시청시작           INT
        , family_영화검색               INT
        , family_영화구매               INT
        , romance_영화평가              INT
        , romance_영화시청완료          INT
        , romance_영화시청시작          INT
        , romance_영화검색              INT
        , romance_영화구매              INT
        , music_영화평가                INT
        , music_영화시청완료            INT
        , music_영화시청시작            INT
        , music_영화검색                INT
        , music_영화구매                INT
        , horror_영화평가               INT
        , horror_영화시청완료           INT
        , horror_영화시청시작           INT
        , horror_영화검색               INT
        , horror_영화구매               INT
        , war_영화평가                  INT
        , war_영화시청완료              INT
        , war_영화시청시작              INT
        , war_영화검색                  INT
        , war_영화구매                  INT
        , western_영화평가              INT
        , western_영화시청완료          INT
        , western_영화시청시작          INT
        , western_영화검색              INT
        , western_영화구매              INT
        , mystery_영화평가              INT
        , mystery_영화시청완료          INT
        , mystery_영화시청시작          INT
        , mystery_영화검색              INT
        , mystery_영화구매              INT
        , short_영화평가                INT
        , short_영화시청완료            INT
        , short_영화시청시작            INT
        , short_영화검색                INT
        , short_영화구매                INT
        , musical_영화평가              INT
        , musical_영화시청완료          INT
        , musical_영화시청시작          INT
        , musical_영화검색              INT
        , musical_영화구매              INT
        , sport_영화평가                INT
        , sport_영화시청완료            INT
        , sport_영화시청시작            INT
        , sport_영화검색                INT
        , sport_영화구매                INT
        , scifi_영화평가                INT
        , scifi_영화시청완료            INT
        , scifi_영화시청시작            INT
        , scifi_영화검색                INT
        , scifi_영화구매                INT
        , biography_영화평가            INT
        , biography_영화시청완료        INT
        , biography_영화시청시작        INT
        , biography_영화검색            INT
        , biography_영화구매            INT
        , news_영화평가                 INT
        , news_영화시청완료             INT
        , news_영화시청시작             INT
        , news_영화검색                 INT
        , news_영화구매                 INT
        , 나라ID                        INT
        , 나이						    INT
        , 대륙ID						text
        , 통근거리					    INT
        , 교육						    text
        , 성별						    text
        , 가구규모					    INT
        , 직업종류					    text
        , 결혼여부					    text
        , 애완동물					    text
        , 고객이_된_년수				INT
        , 이벤트여부                    INT 
        )                          
        """
)

          

cur.execute(
    f"""
    INSERT INTO test_data_set2
    select 
          a1.고객ID
        , a1.기준년월
        , a1.history_영화평가
        , a1.history_영화시청완료
        , a1.history_영화시청시작
        , a1.history_영화검색
        , a1.history_영화구매
        , a1.animation_영화평가
        , a1.animation_영화시청완료
        , a1.animation_영화시청시작
        , a1.animation_영화검색
        , a1.animation_영화구매
        , a1.drama_영화평가
        , a1.drama_영화시청완료
        , a1.drama_영화시청시작
        , a1.drama_영화검색
        , a1.drama_영화구매
        , a1.comedy_영화평가
        , a1.comedy_영화시청완료
        , a1.comedy_영화시청시작
        , a1.comedy_영화검색
        , a1.comedy_영화구매
        , a1.action_영화평가
        , a1.action_영화시청완료
        , a1.action_영화시청시작
        , a1.action_영화검색
        , a1.action_영화구매
        , a1.crime_영화평가
        , a1.crime_영화시청완료
        , a1.crime_영화시청시작
        , a1.crime_영화검색
        , a1.crime_영화구매
        , a1.thriller_영화평가
        , a1.thriller_영화시청완료
        , a1.thriller_영화시청시작
        , a1.thriller_영화검색
        , a1.thriller_영화구매
        , a1.documentary_영화평가
        , a1.documentary_영화시청완료
        , a1.documentary_영화시청시작
        , a1.documentary_영화검색
        , a1.documentary_영화구매
        , a1.adventure_영화평가
        , a1.adventure_영화시청완료
        , a1.adventure_영화시청시작
        , a1.adventure_영화검색
        , a1.adventure_영화구매
        , a1.fantasy_영화평가
        , a1.fantasy_영화시청완료
        , a1.fantasy_영화시청시작
        , a1.fantasy_영화검색
        , a1.fantasy_영화구매
        , a1.family_영화평가
        , a1.family_영화시청완료
        , a1.family_영화시청시작
        , a1.family_영화검색
        , a1.family_영화구매
        , a1.romance_영화평가
        , a1.romance_영화시청완료
        , a1.romance_영화시청시작
        , a1.romance_영화검색
        , a1.romance_영화구매
        , a1.music_영화평가
        , a1.music_영화시청완료
        , a1.music_영화시청시작
        , a1.music_영화검색
        , a1.music_영화구매
        , a1.horror_영화평가
        , a1.horror_영화시청완료
        , a1.horror_영화시청시작
        , a1.horror_영화검색
        , a1.horror_영화구매
        , a1.war_영화평가
        , a1.war_영화시청완료
        , a1.war_영화시청시작
        , a1.war_영화검색
        , a1.war_영화구매
        , a1.western_영화평가
        , a1.western_영화시청완료
        , a1.western_영화시청시작
        , a1.western_영화검색
        , a1.western_영화구매
        , a1.mystery_영화평가
        , a1.mystery_영화시청완료
        , a1.mystery_영화시청시작
        , a1.mystery_영화검색
        , a1.mystery_영화구매
        , a1.short_영화평가
        , a1.short_영화시청완료
        , a1.short_영화시청시작
        , a1.short_영화검색
        , a1.short_영화구매
        , a1.musical_영화평가
        , a1.musical_영화시청완료
        , a1.musical_영화시청시작
        , a1.musical_영화검색
        , a1.musical_영화구매
        , a1.sport_영화평가
        , a1.sport_영화시청완료
        , a1.sport_영화시청시작
        , a1.sport_영화검색
        , a1.sport_영화구매
        , a1.scifi_영화평가
        , a1.scifi_영화시청완료
        , a1.scifi_영화시청시작
        , a1.scifi_영화검색
        , a1.scifi_영화구매
        , a1.biography_영화평가
        , a1.biography_영화시청완료
        , a1.biography_영화시청시작
        , a1.biography_영화검색
        , a1.biography_영화구매
        , a1.news_영화평가
        , a1.news_영화시청완료
        , a1.news_영화시청시작
        , a1.news_영화검색
        , a1.news_영화구매
        , a1.나라ID
        , a1.나이
        , a1.대륙ID
        , a1.통근거리
        , a1.교육
        , a1.성별
        , a1.가구규모
        , a1.직업종류
        , a1.결혼여부
        , a1.애완동물
        , a1.고객이_된_년수
        , a2.scifi 
    from (
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
             , 대륙ID
             , 통근거리
             , 교육
             , 성별
             , 가구규모
             , 직업종류
             , 결혼여부
             , 애완동물
             , 고객이_된_년수
        from 피처마트
        where 기준년월 = '201208'
    ) a1 
    inner join (
        select 
              고객ID
            , 기준년월
            , scifi
        from 타겟마트2
        where 기준년월 = '201209'
          and six_month_active = '1'
    ) a2
    on a1.고객ID = a2.고객ID
    """
)

cnt_1 = pd.read_sql(
    f"""
    select count(*)       as cnt_1
    from test_data_set2
    """
,conn
)

# print('insert 확인')
        
        
print(f"> insert : {cnt_1['cnt_1'][0]}건 확인")
    
DB건수 = pd.read_sql(
    """
    select count(1) as 건수
    from test_data_set2
    """
, conn
).values[0][0]

print(rf"[LOG] DB건수 = {DB건수:,}")


