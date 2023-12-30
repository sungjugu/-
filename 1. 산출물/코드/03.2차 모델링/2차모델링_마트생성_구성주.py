################################
# 0. import
################################
import pandas as pd
import numpy as np
import sqlite3
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


################################
# 1. DB연동
################################
# 일반연결
conn = sqlite3.connect("pine.db", isolation_level=None)
cur = conn.cursor()

# SQLAlchemy 연결
engine = create_engine('sqlite:///pine.db')
conn2 = engine.connect()


################################
# 2. 타겟마트 만들기 (6개월, 1년 휴면고객 탐색)
################################
baseym = ['201209', '201208', '201207', '201206', '201205', '201204', '201203', '201202', '201201',
          '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101',
          '201012', '201011', '201010', '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001']
          
cur.execute(
    f"""
    drop table if exists 타겟마트2
    """
)

cur.execute(
        f"""
        create table if not exists 타겟마트2(
          고객ID                     text
        , 기준년월                   INT
        , history                    text
        , animation                  text
        , drama                      text
        , comedy                     text
        , action                     text
        , crime                      text
        , thriller                   text
        , documentary                text
        , adventure                  text
        , fantasy                    text
        , family                     text
        , romance                    text
        , music                      text
        , horror                     text
        , war                        text
        , western                    text
        , mystery                    text
        , short                      text
        , musical                    text
        , sport                      text
        , scifi                      text
        , biography                  text
        , news                       text
        , six_month_active                  text
        , one_year_active                    text
        )                          
        """
)
datetime_format = '%Y%m'

for ym in baseym : 
    before_base_list = datetime.strptime(ym, datetime_format)
    formatted_date_현재 = before_base_list.strftime("%Y%m")
    
    before_base_list = datetime.strptime(ym, datetime_format)
    six_months_ago = before_base_list - relativedelta(months=6)
    ym_six_bf = six_months_ago.strftime("%Y%m")
    
    one_year_later = before_base_list - relativedelta(years=1)
    ym_one_bf = one_year_later.strftime("%Y%m") 

    cur.execute(
        f"""
        INSERT INTO 타겟마트2
        select
              c.고객ID
            , c.기준년월
            , max(case when b.genre_history     = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as history
            , max(case when b.genre_animation   = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as animation
            , max(case when b.genre_drama       = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as drama
            , max(case when b.genre_comedy      = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as comedy
            , max(case when b.genre_action      = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as action
            , max(case when b.genre_crime       = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as crime
            , max(case when b.genre_thriller    = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as thriller
            , max(case when b.genre_documentary = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as documentary
            , max(case when b.genre_adventure   = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as adventure
            , max(case when b.genre_fantasy     = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as fantasy
            , max(case when b.genre_family      = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as family
            , max(case when b.genre_romance     = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as romance
            , max(case when b.genre_music       = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as music
            , max(case when b.genre_horror      = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as horror
            , max(case when b.genre_war         = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as war
            , max(case when b.genre_western     = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as western
            , max(case when b.genre_mystery     = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as mystery
            , max(case when b.genre_short       = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as short
            , max(case when b.genre_musical     = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as musical
            , max(case when b.genre_sport       = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as sport
            , max(case when b.genre_scifi       = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as scifi
            , max(case when b.genre_biography   = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as biography
            , max(case when b.genre_news        = 1 and b.영화시청완료 = 1 and b.영화시청시작 = 1 and b.영화구매 = 1 then 1 else 0 end) as news
            , case 
                when c.고객ID in (select a1.고객ID from MOVIE_FACT a1 where a1.기준년월 between {ym_six_bf} and {ym}) then 1
                else 0
              end as six_month_active
            , case 
                when c.고객ID in (select a1.고객ID from MOVIE_FACT a1 where a1.기준년월 between {ym_one_bf} and {ym}) then 1
                else 0
              end as one_year_active
              
        from (
            select 
                   '{ym}'               as 기준년월 
                  , 고객ID
            from CUSTOMER
        ) c 
        left join (
                select 
                      기준년월
                    , a1.고객ID
                    , a1.영화ID
                    , a1.추천여부
                    , a1.영화평가
                    , a1.영화시청완료
                    , a1.영화시청시작
                    , a1.영화검색
                    , a1.영화구매
                    , a1.순위
                    , a1.sales
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
                from(     
                    select
                          기준년월
                        , 고객ID
                        , 영화ID
                        , max(case when 추천여부 = '1'  then 1 else 0     end) as 추천여부
                        , max(case when 활동ID =   '1'  then 1 else 0     end) as 영화평가
                        , max(case when 활동ID =   '2'  then 1 else 0     end) as 영화시청완료
                        , max(case when 활동ID =   '4'  then 1 else 0     end) as 영화시청시작
                        , max(case when 활동ID =   '5'  then 1 else 0     end) as 영화검색
                        , max(case when 활동ID =   '11' then 1 else 0     end) as 영화구매
                        , max(case when 순위   is  null then 0 else 순위  end) as 순위
                        , sum(case when sales  is  null then 0 else sales end) as sales
                    from MOVIE_FACT
                    where 기준년월 = '{ym}'
                    group by
                          기준년월
                        , 고객ID
                        , 영화ID
                ) a1 
                left join (
                    select
                          영화ID
                        , max(case when 장르ID = '1'           then 1 else 0 end) as genre_history
                        , max(case when 장르ID = '2'           then 1 else 0 end) as genre_animation
                        , max(case when 장르ID = '3'           then 1 else 0 end) as genre_drama
                        , max(case when 장르ID = '6'           then 1 else 0 end) as genre_comedy
                        , max(case when 장르ID = '7'           then 1 else 0 end) as genre_action
                        , max(case when 장르ID = '8'           then 1 else 0 end) as genre_crime
                        , max(case when 장르ID = '9'           then 1 else 0 end) as genre_thriller
                        , max(case when 장르ID = '10'          then 1 else 0 end) as genre_documentary
                        , max(case when 장르ID = '11'          then 1 else 0 end) as genre_adventure
                        , max(case when 장르ID = '12'          then 1 else 0 end) as genre_fantasy
                        , max(case when 장르ID = '14'          then 1 else 0 end) as genre_family
                        , max(case when 장르ID = '15'          then 1 else 0 end) as genre_romance
                        , max(case when 장르ID = '16'          then 1 else 0 end) as genre_music
                        , max(case when 장르ID = '17'          then 1 else 0 end) as genre_horror
                        , max(case when 장르ID = '18'          then 1 else 0 end) as genre_war
                        , max(case when 장르ID = '19'          then 1 else 0 end) as genre_western
                        , max(case when 장르ID = '20'          then 1 else 0 end) as genre_mystery
                        , max(case when 장르ID = '24'          then 1 else 0 end) as genre_short
                        , max(case when 장르ID = '25'          then 1 else 0 end) as genre_musical
                        , max(case when 장르ID = '30'          then 1 else 0 end) as genre_sport
                        , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                        , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                        , max(case when 장르ID = '45'          then 1 else 0 end) as genre_scifi
                        , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                        , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                        , max(case when 장르ID = '50'          then 1 else 0 end) as genre_lifestyle
                        , max(case when 장르ID = '52'          then 1 else 0 end) as genre_talkshow
                        , max(case when 장르ID = '56'          then 1 else 0 end) as genre_noir
                        , max(case when 장르ID = '-1'          then 1 else 0 end) as genre_unknown
                    from MOVIE_GENRE
                    group by 영화ID
                 ) a2 
                    on
                        a1.영화ID = a2.영화ID
        where 기준년월 = {ym} 
        ) b 
            on
                b.기준년월    =  c.기준년월
            and b.고객ID      =  c.고객ID
        group by 1,2
        """
    )
            
    # print('insert 완료')
            
    cnt_1 = pd.read_sql(
        f"""
        select 
            count(*)          as cnt_1
        from 타겟마트2
        where 기준년월 = {ym}
        """
    ,conn
    )
    
    # print('insert 확인')
            
            
    print(f"{ym} > insert : {cnt_1['cnt_1'][0]}건 확인")
    
DB건수 = pd.read_sql(
    """
    select 
        count(1)              as 건수
    from 타겟마트2
    """
, conn
).values[0][0]

print(rf"[LOG] DB건수 = {DB건수:,}")