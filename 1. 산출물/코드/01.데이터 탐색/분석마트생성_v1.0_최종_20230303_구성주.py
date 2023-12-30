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
# 2. 타겟마트 만들기
################################
baseym = ['201209', '201208', '201207', '201206', '201205', '201204', '201203', '201202', '201201',
          '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101',
          '201012', '201011', '201010', '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001']
          
cur.execute(
    f"""
    drop table if exists 타겟마트
    """
)
    
cur.execute(
        f"""
        create table if not exists 타겟마트(
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
        )                          
        """
)

for ym in baseym : 
    

    cur.execute(
        f"""
        INSERT INTO 타겟마트
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
        from 타겟마트
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
    from 타겟마트
    """
, conn
).values[0][0]

print(rf"[LOG] DB건수 = {DB건수:,}")


################################
# 2. 피처마트 만들기
################################
baseym = ['201209', '201208', '201207', '201206', '201205', '201204', '201203', '201202', '201201',
          '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101',
          '201012', '201011', '201010', '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001']
cur.execute(
    f"""
    drop table if exists 피처마트
    """
)
    
cur.execute(
        f"""
        create table if not exists 피처마트(
          고객ID                        text
        , 기준년월                      INT
        , 평가                          INT
        , recommended                   text
        , history_영화평가              text
        , history_영화시청완료          text
        , history_영화시청시작          text
        , history_영화검색              text
        , history_영화구매              text
        , animation_영화평가            text
        , animation_영화시청완료        text
        , animation_영화시청시작        text
        , animation_영화검색            text
        , animation_영화구매            text
        , drama_영화평가                text
        , drama_영화시청완료            text
        , drama_영화시청시작            text
        , drama_영화검색                text
        , drama_영화구매                text
        , comedy_영화평가               text
        , comedy_영화시청완료           text
        , comedy_영화시청시작           text
        , comedy_영화검색               text
        , comedy_영화구매               text
        , action_영화평가               text
        , action_영화시청완료           text
        , action_영화시청시작           text
        , action_영화검색               text
        , action_영화구매               text
        , crime_영화평가                text
        , crime_영화시청완료            text
        , crime_영화시청시작            text
        , crime_영화검색                text
        , crime_영화구매                text
        , thriller_영화평가             text
        , thriller_영화시청완료         text
        , thriller_영화시청시작         text
        , thriller_영화검색             text
        , thriller_영화구매             text
        , documentary_영화평가          text
        , documentary_영화시청완료      text
        , documentary_영화시청시작      text
        , documentary_영화검색          text
        , documentary_영화구매          text
        , adventure_영화평가            text
        , adventure_영화시청완료        text
        , adventure_영화시청시작        text
        , adventure_영화검색            text
        , adventure_영화구매            text
        , fantasy_영화평가              text
        , fantasy_영화시청완료          text
        , fantasy_영화시청시작          text
        , fantasy_영화검색              text
        , fantasy_영화구매              text
        , family_영화평가               text
        , family_영화시청완료           text
        , family_영화시청시작           text
        , family_영화검색               text
        , family_영화구매               text
        , romance_영화평가              text
        , romance_영화시청완료          text
        , romance_영화시청시작          text
        , romance_영화검색              text
        , romance_영화구매              text
        , music_영화평가                text
        , music_영화시청완료            text
        , music_영화시청시작            text
        , music_영화검색                text
        , music_영화구매                text
        , horror_영화평가               text
        , horror_영화시청완료           text
        , horror_영화시청시작           text
        , horror_영화검색               text
        , horror_영화구매               text
        , war_영화평가                  text
        , war_영화시청완료              text
        , war_영화시청시작              text
        , war_영화검색                  text
        , war_영화구매                  text
        , western_영화평가              text
        , western_영화시청완료          text
        , western_영화시청시작          text
        , western_영화검색              text
        , western_영화구매              text
        , mystery_영화평가              text
        , mystery_영화시청완료          text
        , mystery_영화시청시작          text
        , mystery_영화검색              text
        , mystery_영화구매              text
        , short_영화평가                text
        , short_영화시청완료            text
        , short_영화시청시작            text
        , short_영화검색                text
        , short_영화구매                text
        , musical_영화평가              text
        , musical_영화시청완료          text
        , musical_영화시청시작          text
        , musical_영화검색              text
        , musical_영화구매              text
        , sport_영화평가                text
        , sport_영화시청완료            text
        , sport_영화시청시작            text
        , sport_영화검색                text
        , sport_영화구매                text
        , scifi_영화평가                text
        , scifi_영화시청완료            text
        , scifi_영화시청시작            text
        , scifi_영화검색                text
        , scifi_영화구매                text
        , biography_영화평가            text
        , biography_영화시청완료        text
        , biography_영화시청시작        text
        , biography_영화검색            text
        , biography_영화구매            text
        , news_영화평가                 text
        , news_영화시청완료             text
        , news_영화시청시작             text
        , news_영화검색                 text
        , news_영화구매                 text
        , 나라ID                        text
        , 나이                          text
        , 대륙ID                        text
        , 통근거리                      INT
        , 교육                          text
        , 성별                          text
        , 가구규모                      text
        , 직업종류                      text
        , 결혼여부                      text
        , 애완동물                      text
        , 프로모션응답                  text
        , 고객이_된_년수                INT
        )                          
        """
)


for ym in baseym : 
    

    cur.execute(
        f"""
        INSERT INTO 피처마트
        select
              b.고객ID
            , b.기준년월
            , b.순위
            , b.추천여부
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
            , c.대륙ID
            , c.통근거리
            , c.교육
            , c.성별
            , c.가구규모
            , c.직업종류
            , c.결혼여부
            , c.애완동물
            , c.프로모션응답
            , c.고객이_된_년수
        from (
            select 
                  '{ym}' as 기준년월
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
                where 기준년월 = '{ym}'
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
            where 기준년월 = {ym} 
        ) b 
        on
            b.기준년월 = c.기준년월 and b.고객ID = c.고객ID
        group by 1,2
        """
    )
            
    # print('insert 완료')
            
    cnt_1 = pd.read_sql(
        f"""
        select count(*)       as cnt_1
        from 피처마트
        where 기준년월 = {ym}
        """
    ,conn
    )
    
    # print('insert 확인')
            
            
    print(f"{ym} > insert : {cnt_1['cnt_1'][0]}건 확인")
    
DB건수 = pd.read_sql(
    """
    select count(1)           as 건수
    from 피처마트
    """
, conn
).values[0][0]

print(rf"[LOG] DB건수 = {DB건수:,}")


################################
# 3. 분석마트 만들기
################################
baseym = ['201208', '201207', '201206', '201205', '201204', '201203', '201202', '201201',
          '201112', '201111', '201110', '201109', '201108', '201107', '201106', '201105', '201104', '201103', '201102', '201101',
          '201012', '201011', '201010', '201009', '201008', '201007', '201006', '201005', '201004', '201003', '201002', '201001']
          
cur.execute(
    f"""
    drop table if exists 분석마트
    """
)

cur.execute(
        f"""
        create table if not exists 분석마트(
          고객ID						text
        , 기준년월					    INT
        , 평가						    INT
        , recommended                   text
        , history_영화평가			    text
        , history_영화시청완료          text
        , history_영화시청시작          text
        , history_영화검색              text
        , history_영화구매              text
        , animation_영화평가            text
        , animation_영화시청완료        text
        , animation_영화시청시작        text
        , animation_영화검색            text
        , animation_영화구매            text
        , drama_영화평가                text
        , drama_영화시청완료            text
        , drama_영화시청시작            text
        , drama_영화검색                text
        , drama_영화구매                text
        , comedy_영화평가               text
        , comedy_영화시청완료           text
        , comedy_영화시청시작           text
        , comedy_영화검색               text
        , comedy_영화구매               text
        , action_영화평가               text
        , action_영화시청완료           text
        , action_영화시청시작           text
        , action_영화검색               text
        , action_영화구매               text
        , crime_영화평가                text
        , crime_영화시청완료            text
        , crime_영화시청시작            text
        , crime_영화검색                text
        , crime_영화구매                text
        , thriller_영화평가             text
        , thriller_영화시청완료         text
        , thriller_영화시청시작         text
        , thriller_영화검색             text
        , thriller_영화구매             text
        , documentary_영화평가          text
        , documentary_영화시청완료      text
        , documentary_영화시청시작      text
        , documentary_영화검색          text
        , documentary_영화구매          text
        , adventure_영화평가            text
        , adventure_영화시청완료        text
        , adventure_영화시청시작        text
        , adventure_영화검색            text
        , adventure_영화구매            text
        , fantasy_영화평가              text
        , fantasy_영화시청완료          text
        , fantasy_영화시청시작          text
        , fantasy_영화검색              text
        , fantasy_영화구매              text
        , family_영화평가               text
        , family_영화시청완료           text
        , family_영화시청시작           text
        , family_영화검색               text
        , family_영화구매               text
        , romance_영화평가              text
        , romance_영화시청완료          text
        , romance_영화시청시작          text
        , romance_영화검색              text
        , romance_영화구매              text
        , music_영화평가                text
        , music_영화시청완료            text
        , music_영화시청시작            text
        , music_영화검색                text
        , music_영화구매                text
        , horror_영화평가               text
        , horror_영화시청완료           text
        , horror_영화시청시작           text
        , horror_영화검색               text
        , horror_영화구매               text
        , war_영화평가                  text
        , war_영화시청완료              text
        , war_영화시청시작              text
        , war_영화검색                  text
        , war_영화구매                  text
        , western_영화평가              text
        , western_영화시청완료          text
        , western_영화시청시작          text
        , western_영화검색              text
        , western_영화구매              text
        , mystery_영화평가              text
        , mystery_영화시청완료          text
        , mystery_영화시청시작          text
        , mystery_영화검색              text
        , mystery_영화구매              text
        , short_영화평가                text
        , short_영화시청완료            text
        , short_영화시청시작            text
        , short_영화검색                text
        , short_영화구매                text
        , musical_영화평가              text
        , musical_영화시청완료          text
        , musical_영화시청시작          text
        , musical_영화검색              text
        , musical_영화구매              text
        , sport_영화평가                text
        , sport_영화시청완료            text
        , sport_영화시청시작            text
        , sport_영화검색                text
        , sport_영화구매                text
        , scifi_영화평가                text
        , scifi_영화시청완료            text
        , scifi_영화시청시작            text
        , scifi_영화검색                text
        , scifi_영화구매                text
        , biography_영화평가            text
        , biography_영화시청완료        text
        , biography_영화시청시작        text
        , biography_영화검색            text
        , biography_영화구매            text
        , news_영화평가                 text
        , news_영화시청완료             text
        , news_영화시청시작             text
        , news_영화검색                 text
        , news_영화구매                 text
        , history                       text
        , animation                     text
        , drama                         text
        , comedy                        text
        , action                        text
        , crime                         text
        , thriller                      text
        , documentary                   text
        , adventure                     text
        , fantasy                       text
        , family                        text
        , romance                       text
        , music                         text
        , horror                        text
        , war                           text
        , western                       text
        , mystery                       text
        , short                         text
        , musical                       text
        , sport                         text
        , scifi                         text
        , biography                     text
        , news                          text
        , 나라ID                        text
        , 나이						    INT
        , 대륙ID						text
        , 통근거리					    INT
        , 교육						    text
        , 성별						    text
        , 가구규모					    INT
        , 직업종류					    text
        , 결혼여부					    text
        , 애완동물					    text
        , 프로모션응답				    text
        , 고객이_된_년수				INT
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
        INSERT INTO 분석마트
        select 
              a1.고객ID
            , a1.기준년월
            , a1.평가
            , a1.recommended
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
            , a2.history    
            , a2.animation  
            , a2.drama      
            , a2.comedy     
            , a2.action     
            , a2.crime      
            , a2.thriller   
            , a2.documentary
            , a2.adventure  
            , a2.fantasy    
            , a2.family     
            , a2.romance    
            , a2.music      
            , a2.horror     
            , a2.war        
            , a2.western    
            , a2.mystery    
            , a2.short      
            , a2.musical    
            , a2.sport      
            , a2.scifi      
            , a2.biography  
            , a2.news
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
            , a1.프로모션응답
            , a1.고객이_된_년수
        from (
            select 
                   고객ID
                 , 기준년월
                 , 평가
                 , recommended
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
                 , 프로모션응답
                 , 고객이_된_년수
            from 피처마트
            where 기준년월 = '{ym}'
        ) a1 
        left join (
            select 
                  고객ID     
                ,기준년월   
                ,history    
                ,animation  
                ,drama      
                ,comedy     
                ,action     
                ,crime      
                ,thriller   
                ,documentary
                ,adventure  
                ,fantasy    
                ,family     
                ,romance    
                ,music      
                ,horror     
                ,war        
                ,western    
                ,mystery    
                ,short      
                ,musical    
                ,sport      
                ,scifi      
                ,biography  
                ,news     
            from 타겟마트
            where 기준년월 = '{ym_af}'
        ) a2
        on a1.고객ID = a2.고객ID
        """
    )
       
            
    cnt_1 = pd.read_sql(
        f"""
        select count(*)       as cnt_1
        from 분석마트
        where 기준년월 = '{ym}'
        """
    ,conn
    )
    
    # print('insert 확인')
            
            
    print(f"{ym} > insert : {cnt_1['cnt_1'][0]}건 확인")
    
DB건수 = pd.read_sql(
    """
    select count(1) as 건수
    from 분석마트
    """
, conn
).values[0][0]

print(rf"[LOG] DB건수 = {DB건수:,}")