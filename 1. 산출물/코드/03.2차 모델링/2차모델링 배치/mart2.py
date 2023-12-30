############################################################
# 파일명  mart.py
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
import pandas as pd
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
six_months_ago = dt - relativedelta(months=7)
one_year_later = dt - relativedelta(years=1) - relativedelta(months=1)
dt_minus_one_month = dt_minus_one_month.strftime('%Y%m')
ym_six_bf = six_months_ago.strftime("%Y%m")
ym_one_bf = one_year_later.strftime("%Y%m") 

print(f'[LOG] [{str(start).split(".")[0]}] mart - 프로그램 시작 - 기준년월 {dt_minus_one_month}')


################################
# 3. DB 연결
################################
st_tm_tot = datetime.now()
print(f'[LOG] [{str(st_tm_tot).split(".")[0]}] mart - STEP01 DB 연결 시작]')

conn = sqlite3.connect(f"{db_dir}", isolation_level=None)
cur = conn.cursor()

engine = create_engine('sqlite:///pine.db')
conn2 = engine.connect()

ed_tm_tot = datetime.now() 
el_tm_tot = ed_tm_tot - st_tm_tot 
print(f'[LOG] [{str(ed_tm_tot).split(".")[0]}] mart - STEP01 DB 연결 완료]')
print(f'[LOG] [DB연결 소요시간 : {str(el_tm_tot).split(".")[0]}]')


################################
# 4. 마트생성 및 적재
################################
cur.execute(
    f"""
    delete
    from 피처마트2
    where 기준년월 = {dt_minus_one_month}
    """
)

st_tm_tot = datetime.now()
print(f'[LOG] [{str(st_tm_tot).split(".")[0]}] mart - STEP02 DB 적재 시작]')
cur.execute(
    f"""
    INSERT INTO 피처마트2
    select
          c.고객ID
        , c.기준년월
        , max(case when b.genre_history             = 1  and b.영화평가     = 1 then 1 else 0 end) as history_영화평가
        , max(case when b.genre_history             = 1  and b.영화시청완료 = 1 then 1 else 0 end) as history_영화시청완료
        , max(case when b.genre_history             = 1  and b.영화시청시작 = 1 then 1 else 0 end) as history_영화시청시작
        , max(case when b.genre_history             = 1  and b.영화검색     = 1 then 1 else 0 end) as history_영화검색
        , max(case when b.genre_history             = 1  and b.영화구매     = 1 then 1 else 0 end) as history_영화구매
        , max(case when b.genre_animation           = 1  and b.영화평가     = 1 then 1 else 0 end) as animation_영화평가
        , max(case when b.genre_animation           = 1  and b.영화시청완료 = 1 then 1 else 0 end) as animation_영화시청완료
        , max(case when b.genre_animation           = 1  and b.영화시청시작 = 1 then 1 else 0 end) as animation_영화시청시작
        , max(case when b.genre_animation           = 1  and b.영화검색     = 1 then 1 else 0 end) as animation_영화검색
        , max(case when b.genre_animation           = 1  and b.영화구매     = 1 then 1 else 0 end) as animation_영화구매
        , max(case when b.genre_drama               = 1  and b.영화평가     = 1 then 1 else 0 end) as drama_영화평가
        , max(case when b.genre_drama               = 1  and b.영화시청완료 = 1 then 1 else 0 end) as drama_영화시청완료
        , max(case when b.genre_drama               = 1  and b.영화시청시작 = 1 then 1 else 0 end) as drama_영화시청시작
        , max(case when b.genre_drama               = 1  and b.영화검색     = 1 then 1 else 0 end) as drama_영화검색
        , max(case when b.genre_drama               = 1  and b.영화구매     = 1 then 1 else 0 end) as drama_영화구매
        , max(case when b.genre_comedy              = 1  and b.영화평가     = 1 then 1 else 0 end) as comedy_영화평가
        , max(case when b.genre_comedy              = 1  and b.영화시청완료 = 1 then 1 else 0 end) as comedy_영화시청완료
        , max(case when b.genre_comedy              = 1  and b.영화시청시작 = 1 then 1 else 0 end) as comedy_영화시청시작
        , max(case when b.genre_comedy              = 1  and b.영화검색     = 1 then 1 else 0 end) as comedy_영화검색
        , max(case when b.genre_comedy              = 1  and b.영화구매     = 1 then 1 else 0 end) as comedy_영화구매
        , max(case when b.genre_action              = 1  and b.영화평가     = 1 then 1 else 0 end) as action_영화평가
        , max(case when b.genre_action              = 1  and b.영화시청완료 = 1 then 1 else 0 end) as action_영화시청완료
        , max(case when b.genre_action              = 1  and b.영화시청시작 = 1 then 1 else 0 end) as action_영화시청시작
        , max(case when b.genre_action              = 1  and b.영화검색     = 1 then 1 else 0 end) as action_영화검색
        , max(case when b.genre_action              = 1  and b.영화구매     = 1 then 1 else 0 end) as action_영화구매
        , max(case when b.genre_crime               = 1  and b.영화평가     = 1 then 1 else 0 end) as crime_영화평가
        , max(case when b.genre_crime               = 1  and b.영화시청완료 = 1 then 1 else 0 end) as crime_영화시청완료
        , max(case when b.genre_crime               = 1  and b.영화시청시작 = 1 then 1 else 0 end) as crime_영화시청시작
        , max(case when b.genre_crime               = 1  and b.영화검색     = 1 then 1 else 0 end) as crime_영화검색
        , max(case when b.genre_crime               = 1  and b.영화구매     = 1 then 1 else 0 end) as crime_영화구매
        , max(case when b.genre_thriller            = 1  and b.영화평가     = 1 then 1 else 0 end) as thriller_영화평가
        , max(case when b.genre_thriller            = 1  and b.영화시청완료 = 1 then 1 else 0 end) as thriller_영화시청완료
        , max(case when b.genre_thriller            = 1  and b.영화시청시작 = 1 then 1 else 0 end) as thriller_영화시청시작
        , max(case when b.genre_thriller            = 1  and b.영화검색     = 1 then 1 else 0 end) as thriller_영화검색
        , max(case when b.genre_thriller            = 1  and b.영화구매     = 1 then 1 else 0 end) as thriller_영화구매
        , max(case when b.genre_documentary         = 1  and b.영화평가     = 1 then 1 else 0 end) as documentary_영화평가
        , max(case when b.genre_documentary         = 1  and b.영화시청완료 = 1 then 1 else 0 end) as documentary_영화시청완료
        , max(case when b.genre_documentary         = 1  and b.영화시청시작 = 1 then 1 else 0 end) as documentary_영화시청시작
        , max(case when b.genre_documentary         = 1  and b.영화검색     = 1 then 1 else 0 end) as documentary_영화검색
        , max(case when b.genre_documentary         = 1  and b.영화구매     = 1 then 1 else 0 end) as documentary_영화구매
        , max(case when b.genre_adventure           = 1  and b.영화평가     = 1 then 1 else 0 end) as adventure_영화평가
        , max(case when b.genre_adventure           = 1  and b.영화시청완료 = 1 then 1 else 0 end) as adventure_영화시청완료
        , max(case when b.genre_adventure           = 1  and b.영화시청시작 = 1 then 1 else 0 end) as adventure_영화시청시작
        , max(case when b.genre_adventure           = 1  and b.영화검색     = 1 then 1 else 0 end) as adventure_영화검색
        , max(case when b.genre_adventure           = 1  and b.영화구매     = 1 then 1 else 0 end) as adventure_영화구매
        , max(case when b.genre_fantasy             = 1  and b.영화평가     = 1 then 1 else 0 end) as fantasy_영화평가
        , max(case when b.genre_fantasy             = 1  and b.영화시청완료 = 1 then 1 else 0 end) as fantasy_영화시청완료
        , max(case when b.genre_fantasy             = 1  and b.영화시청시작 = 1 then 1 else 0 end) as fantasy_영화시청시작
        , max(case when b.genre_fantasy             = 1  and b.영화검색     = 1 then 1 else 0 end) as fantasy_영화검색
        , max(case when b.genre_fantasy             = 1  and b.영화구매     = 1 then 1 else 0 end) as fantasy_영화구매
        , max(case when b.genre_family              = 1  and b.영화평가     = 1 then 1 else 0 end) as family_영화평가
        , max(case when b.genre_family              = 1  and b.영화시청완료 = 1 then 1 else 0 end) as family_영화시청완료
        , max(case when b.genre_family              = 1  and b.영화시청시작 = 1 then 1 else 0 end) as family_영화시청시작
        , max(case when b.genre_family              = 1  and b.영화검색     = 1 then 1 else 0 end) as family_영화검색
        , max(case when b.genre_family              = 1  and b.영화구매     = 1 then 1 else 0 end) as family_영화구매
        , max(case when b.genre_romance             = 1  and b.영화평가     = 1 then 1 else 0 end) as romance_영화평가
        , max(case when b.genre_romance             = 1  and b.영화시청완료 = 1 then 1 else 0 end) as romance_영화시청완료
        , max(case when b.genre_romance             = 1  and b.영화시청시작 = 1 then 1 else 0 end) as romance_영화시청시작
        , max(case when b.genre_romance             = 1  and b.영화검색 = 1     then 1 else 0 end) as romance_영화검색
        , max(case when b.genre_romance             = 1  and b.영화구매 = 1     then 1 else 0 end) as romance_영화구매
        , max(case when b.genre_music               = 1  and b.영화평가 = 1     then 1 else 0 end) as music_영화평가
        , max(case when b.genre_music               = 1  and b.영화시청완료 = 1 then 1 else 0 end) as music_영화시청완료
        , max(case when b.genre_music               = 1  and b.영화시청시작 = 1 then 1 else 0 end) as music_영화시청시작
        , max(case when b.genre_music               = 1  and b.영화검색 = 1     then 1 else 0 end) as music_영화검색
        , max(case when b.genre_music               = 1  and b.영화구매 = 1     then 1 else 0 end) as music_영화구매
        , max(case when b.genre_horror              = 1  and b.영화평가 = 1     then 1 else 0 end) as horror_영화평가
        , max(case when b.genre_horror              = 1  and b.영화시청완료 = 1 then 1 else 0 end) as horror_영화시청완료
        , max(case when b.genre_horror              = 1  and b.영화시청시작 = 1 then 1 else 0 end) as horror_영화시청시작
        , max(case when b.genre_horror              = 1  and b.영화검색 = 1     then 1 else 0 end) as horror_영화검색
        , max(case when b.genre_horror              = 1  and b.영화구매 = 1     then 1 else 0 end) as horror_영화구매
        , max(case when b.genre_war                 = 1  and b.영화평가 = 1     then 1 else 0 end) as war_영화평가
        , max(case when b.genre_war                 = 1  and b.영화시청완료 = 1 then 1 else 0 end) as war_영화시청완료
        , max(case when b.genre_war                 = 1  and b.영화시청시작 = 1 then 1 else 0 end) as war_영화시청시작
        , max(case when b.genre_war                 = 1  and b.영화검색 = 1     then 1 else 0 end) as war_영화검색
        , max(case when b.genre_war                 = 1  and b.영화구매 = 1     then 1 else 0 end) as war_영화구매
        , max(case when b.genre_western             = 1  and b.영화평가 = 1     then 1 else 0 end) as western_영화평가
        , max(case when b.genre_western             = 1  and b.영화시청완료 = 1 then 1 else 0 end) as western_영화시청완료
        , max(case when b.genre_western             = 1  and b.영화시청시작 = 1 then 1 else 0 end) as western_영화시청시작
        , max(case when b.genre_western             = 1  and b.영화검색 = 1     then 1 else 0 end) as western_영화검색
        , max(case when b.genre_western             = 1  and b.영화구매 = 1     then 1 else 0 end) as western_영화구매
        , max(case when b.genre_mystery             = 1  and b.영화평가 = 1     then 1 else 0 end) as mystery_영화평가
        , max(case when b.genre_mystery             = 1  and b.영화시청완료 = 1 then 1 else 0 end) as mystery_영화시청완료
        , max(case when b.genre_mystery             = 1  and b.영화시청시작 = 1 then 1 else 0 end) as mystery_영화시청시작
        , max(case when b.genre_mystery             = 1  and b.영화검색 = 1     then 1 else 0 end) as mystery_영화검색
        , max(case when b.genre_mystery             = 1  and b.영화구매 = 1     then 1 else 0 end) as mystery_영화구매
        , max(case when b.genre_short               = 1  and b.영화평가 = 1     then 1 else 0 end) as short_영화평가
        , max(case when b.genre_short               = 1  and b.영화시청완료 = 1 then 1 else 0 end) as short_영화시청완료
        , max(case when b.genre_short               = 1  and b.영화시청시작 = 1 then 1 else 0 end) as short_영화시청시작
        , max(case when b.genre_short               = 1  and b.영화검색 = 1     then 1 else 0 end) as short_영화검색
        , max(case when b.genre_short               = 1  and b.영화구매 = 1     then 1 else 0 end) as short_영화구매
        , max(case when b.genre_musical             = 1  and b.영화평가 = 1     then 1 else 0 end) as musical_영화평가
        , max(case when b.genre_musical             = 1  and b.영화시청완료 = 1 then 1 else 0 end) as musical_영화시청완료
        , max(case when b.genre_musical             = 1  and b.영화시청시작 = 1 then 1 else 0 end) as musical_영화시청시작
        , max(case when b.genre_musical             = 1  and b.영화검색 = 1     then 1 else 0 end) as musical_영화검색
        , max(case when b.genre_musical             = 1  and b.영화구매 = 1     then 1 else 0 end) as musical_영화구매
        , max(case when b.genre_sport               = 1  and b.영화평가 = 1     then 1 else 0 end) as sport_영화평가
        , max(case when b.genre_sport               = 1  and b.영화시청완료 = 1 then 1 else 0 end) as sport_영화시청완료
        , max(case when b.genre_sport               = 1  and b.영화시청시작 = 1 then 1 else 0 end) as sport_영화시청시작
        , max(case when b.genre_sport               = 1  and b.영화검색 = 1     then 1 else 0 end) as sport_영화검색
        , max(case when b.genre_sport               = 1  and b.영화구매 = 1     then 1 else 0 end) as sport_영화구매
        , max(case when b.genre_scifi               = 1  and b.영화평가 = 1     then 1 else 0 end) as scifi_영화평가
        , max(case when b.genre_scifi               = 1  and b.영화시청완료 = 1 then 1 else 0 end) as scifi_영화시청완료
        , max(case when b.genre_scifi               = 1  and b.영화시청시작 = 1 then 1 else 0 end) as scifi_영화시청시작
        , max(case when b.genre_scifi               = 1  and b.영화검색 = 1     then 1 else 0 end) as scifi_영화검색
        , max(case when b.genre_scifi               = 1  and b.영화구매 = 1     then 1 else 0 end) as scifi_영화구매
        , max(case when b.genre_biography           = 1  and b.영화평가 = 1     then 1 else 0 end) as biography_영화평가
        , max(case when b.genre_biography           = 1  and b.영화시청완료 = 1 then 1 else 0 end) as biography_영화시청완료
        , max(case when b.genre_biography           = 1  and b.영화시청시작 = 1 then 1 else 0 end) as biography_영화시청시작
        , max(case when b.genre_biography           = 1  and b.영화검색 = 1     then 1 else 0 end) as biography_영화검색
        , max(case when b.genre_biography           = 1  and b.영화구매 = 1     then 1 else 0 end) as biography_영화구매
        , max(case when b.genre_news                = 1  and b.영화평가 = 1     then 1 else 0 end) as news_영화평가
        , max(case when b.genre_news                = 1  and b.영화시청완료 = 1 then 1 else 0 end) as news_영화시청완료
        , max(case when b.genre_news                = 1  and b.영화시청시작 = 1 then 1 else 0 end) as news_영화시청시작
        , max(case when b.genre_news                = 1  and b.영화검색 = 1     then 1 else 0 end) as news_영화검색
        , max(case when b.genre_news                = 1  and b.영화구매 = 1     then 1 else 0 end) as news_영화구매
        , c.나라ID
        , c.나이
        , case
                when c.나이 < 20 then '10대'
                when c.나이 < 30 then '20대'
                when c.나이 < 40 then '30대'
                when c.나이 < 50 then '40대'
                when c.나이 < 60 then '50대'
                when c.나이 < 70 then '60대'
                when c.나이 < 80 then '70대'
                when c.나이 < 90 then '80대'
                else c.나이
              end as 연령대
        , c.대륙ID
        , c.통근거리
        , c.교육
        , c.성별
        , c.가구규모
        , c.직업종류
        , c.결혼여부
        , c.애완동물
        , c.고객이_된_년수
        , case 
            when c.고객ID in (select a1.고객ID from MOVIE_FACT a1 where a1.기준년월 between {ym_six_bf} and {dt_minus_one_month}) then 1
            else 0
          end as six_month_active
        , case 
            when c.고객ID in (select a1.고객ID from MOVIE_FACT a1 where a1.기준년월 between {ym_one_bf} and {dt_minus_one_month}) then 1
            else 0
          end as one_year_active
    from (
        select 
              '{dt_minus_one_month}' as 기준년월
            , 고객ID
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
            , 프로모션응답
            , 고객이_된_년수
        from CUSTOMER
    ) c 
    left join (
        select 
              a1.기준년월
            , a1.고객ID
            , a1.영화ID
            , a1.추천여부
            , a1.영화평가
            , a1.영화시청완료
            , a1.영화시청시작
            , a1.영화검색
            , a1.영화구매
            , a1.순위
            , a2.genre_history
            , a2.genre_animation
            , a2.genre_drama
            , a2.genre_comedy
            , a2.genre_action
            , a2.genre_crime
            , a2.genre_thriller
            , a2.genre_documentary
            , a2.genre_adventure
            , a2.genre_fantasy
            , a2.genre_family
            , a2.genre_romance
            , a2.genre_music
            , a2.genre_horror
            , a2.genre_war
            , a2.genre_western
            , a2.genre_mystery
            , a2.genre_short
            , a2.genre_musical
            , a2.genre_sport
            , a2.genre_gameshow
            , a2.genre_reality
            , a2.genre_scifi
            , a2.genre_biography
            , a2.genre_news
            , a2.genre_lifestyle
            , a2.genre_talkshow
            , a2.genre_noir
            , a2.genre_unknown
        from (     
            select
                  기준년월
                , 고객ID
                , 영화ID
                , max(case when 추천여부 = '1'          then 1 else 0 end)     as 추천여부
                , max(case when 활동ID   = '1'          then 1 else 0 end)     as 영화평가
                , max(case when 활동ID   = '2'          then 1 else 0 end)     as 영화시청완료
                , max(case when 활동ID   = '4'          then 1 else 0 end)     as 영화시청시작
                , max(case when 활동ID   = '5'          then 1 else 0 end)     as 영화검색
                , max(case when 활동ID   = '11'         then 1 else 0 end)     as 영화구매
                , max(case when 순위   is null          then 0 else 순위 end)  as 순위
                , sum(case when sales  is null          then 0 else sales end) as sales
            from MOVIE_FACT
            where 기준년월 = '{dt_minus_one_month}'
            group by
                  기준년월
                , 고객ID
                , 영화ID
        ) a1 
        left join (
            select
                  영화ID
                , max(case when 장르ID = '1'            then 1 else 0 end) as genre_history
                , max(case when 장르ID = '2'            then 1 else 0 end) as genre_animation
                , max(case when 장르ID = '3'            then 1 else 0 end) as genre_drama
                , max(case when 장르ID = '6'            then 1 else 0 end) as genre_comedy
                , max(case when 장르ID = '7'            then 1 else 0 end) as genre_action
                , max(case when 장르ID = '8'            then 1 else 0 end) as genre_crime
                , max(case when 장르ID = '9'            then 1 else 0 end) as genre_thriller
                , max(case when 장르ID = '10'           then 1 else 0 end) as genre_documentary
                , max(case when 장르ID = '11'           then 1 else 0 end) as genre_adventure
                , max(case when 장르ID = '12'           then 1 else 0 end) as genre_fantasy
                , max(case when 장르ID = '14'           then 1 else 0 end) as genre_family
                , max(case when 장르ID = '15'           then 1 else 0 end) as genre_romance
                , max(case when 장르ID = '16'           then 1 else 0 end) as genre_music
                , max(case when 장르ID = '17'           then 1 else 0 end) as genre_horror
                , max(case when 장르ID = '18'           then 1 else 0 end) as genre_war
                , max(case when 장르ID = '19'           then 1 else 0 end) as genre_western
                , max(case when 장르ID = '20'           then 1 else 0 end) as genre_mystery
                , max(case when 장르ID = '24'           then 1 else 0 end) as genre_short
                , max(case when 장르ID = '25'           then 1 else 0 end) as genre_musical
                , max(case when 장르ID = '30'           then 1 else 0 end) as genre_sport
                , max(case when 장르ID in ('43', '49')  then 1 else 0 end) as genre_gameshow
                , max(case when 장르ID in ('44', '48')  then 1 else 0 end) as genre_reality
                , max(case when 장르ID = '45'           then 1 else 0 end) as genre_scifi
                , max(case when 장르ID in ('46', '53')  then 1 else 0 end) as genre_biography
                , max(case when 장르ID in ('47', '51')  then 1 else 0 end) as genre_news
                , max(case when 장르ID = '50'           then 1 else 0 end) as genre_lifestyle
                , max(case when 장르ID = '52'           then 1 else 0 end) as genre_talkshow
                , max(case when 장르ID = '56'           then 1 else 0 end) as genre_noir
                , max(case when 장르ID = '-1'           then 1 else 0 end) as genre_unknown
            from MOVIE_GENRE
            group by 영화ID
        ) a2 
        on
            a1.영화ID = a2.영화ID
        where 기준년월 = {dt_minus_one_month} 
    ) b 
    on
        b.기준년월 = c.기준년월 and b.고객ID = c.고객ID
    group by 1,2
    """
)

DB건수 = pd.read_sql(
    f"""
    select count(*)       as cnt_1
    from 피처마트2
    where 기준년월 = {dt_minus_one_month}
    """
,conn
).values[0][0]

ed_tm_tot = datetime.now() 
el_tm_tot = ed_tm_tot - st_tm_tot 
print(f'[LOG] [{str(ed_tm_tot).split(".")[0]}] mart - STEP02 DB 적재 완료]')
print(f'[LOG] [적재 소요시간 : {str(el_tm_tot).split(".")[0]}]')
print(rf"[LOG] [적재 건수 = {DB건수:,}]")


end = datetime.now() 
all_time = end - start 
print(f'[LOG] [{str(end).split(".")[0]}] mart - 프로그램 완료 - 기준년월 {dt_minus_one_month}')
print(f'[LOG] [프로그램 소요시간 : {str(all_time).split(".")[0]}]')


