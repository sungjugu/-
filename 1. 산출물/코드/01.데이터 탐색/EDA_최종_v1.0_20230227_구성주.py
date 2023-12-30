################################
# 1. 기초 현황 파악
################################
#1.1 전체 고객 현황
a = pd.read_sql(
                """
                select 
                     COUNT(*)
                from CUSTOMER 
                
"""
, conn
)
a


#1.2 월별 활동 고객 현황
a = pd.read_sql(
                """
                select 
                     기준년월
                   , count(distinct 고객ID) as 건수
                from MOVIE_FACT
                group by 기준년월
                
"""
, conn
)
a

#1.3 전체 영화 현황
a = pd.read_sql(
                """
                select 
                     count(distinct 영화ID) as 영화개수
                from MOVIE
                
"""
, conn
)
a

#1.4 전체 장르 현황
a = pd.read_sql(
                """
                select 
                     count(distinct 장르ID) as 장르개수
                from MOVIE_GENRE
                
"""
, conn
)
a


################################
# 2. 장르 현황 탐색 및 선정
################################
#2.2.1 장르별 영화 종류 개수
a = pd.read_sql(
                """
                select 
                     COUNT(영화ID) as 영화개수
                     , 장르ID
                from MOVIE_GENRE 
                group by 장르ID
                order by 영화개수 desc
"""
, conn
)
a

#2.2.2 장르별 시청 고객수
a = pd.read_sql("""
                            select
                                  장르ID
                                , count(distinct 고객ID) 고객수
                            from (
                                select
                                    기준년월
                                    , 고객ID
                                    , case when 장르ID in (46, 53) then 46
                                            when 장르ID in (47, 51) then 47 else 장르ID end as 장르ID
                                    , 영화ID
                                from MOVIE_FACT 
                                )
                            where 장르ID != '-1'
                            group by 1
                            order by 고객수 desc
"""
, conn)
a

#2.2.3 장르별 비용 대비 흥행수익
# 평균 수익
a = pd.read_sql(
                """
                select
						avg(case when 장르_history    =1 then 흥행수익 else 0 end) as 수익_history
					  , avg(case when 장르_enimation	 =1 then 흥행수익 else 0 end) as 수익_enimation
					  , avg(case when 장르_drama      =1 then 흥행수익 else 0 end) as 수익_drama
					  , avg(case when 장르_comedy	 =1 then 흥행수익 else 0 end) as 수익_comedy
					  , avg(case when 장르_action	 =1 then 흥행수익 else 0 end) as 수익_action
					  , avg(case when 장르_crime	     =1 then 흥행수익 else 0 end) as 수익_crime
					  , avg(case when 장르_thriller   =1 then 흥행수익 else 0 end) as 수익_thriller
					  , avg(case when 장르_documenta  =1 then 흥행수익 else 0 end) as 수익_documenta
					  , avg(case when 장르_adventure	 =1 then 흥행수익 else 0 end) as 수익_adventure
					  , avg(case when 장르_fantasy	 =1 then 흥행수익 else 0 end) as 수익_fantasy
					  , avg(case when 장르_family	 =1 then 흥행수익 else 0 end) as 수익_family
					  , avg(case when 장르_romance    =1 then 흥행수익 else 0 end) as 수익_romance
					  , avg(case when 장르_music	     =1 then 흥행수익 else 0 end) as 수익_music
					  , avg(case when 장르_horror	 =1 then 흥행수익 else 0 end) as 수익_horror
					  , avg(case when 장르_war	     =1 then 흥행수익 else 0 end) as 수익_war
					  , avg(case when 장르_western	 =1 then 흥행수익 else 0 end) as 수익_western
					  , avg(case when 장르_mystery	 =1 then 흥행수익 else 0 end) as 수익_mystery
					  , avg(case when 장르_short	     =1 then 흥행수익 else 0 end) as 수익_short
					  , avg(case when 장르_musical	 =1 then 흥행수익 else 0 end) as 수익_musical
					  , avg(case when 장르_sport	     =1 then 흥행수익 else 0 end) as 수익_sport
					  , avg(case when 장르_gameshow	 =1 then 흥행수익 else 0 end) as 수익_gameshow
					  , avg(case when 장르_reality	 =1 then 흥행수익 else 0 end) as 수익_reality
					  , avg(case when 장르_sci-fi     =1 then 흥행수익 else 0 end) as 수익_sci-fi
					  , avg(case when 장르_Biography  =1 then 흥행수익 else 0 end) as 수익_Biography
					  , avg(case when 장르_news       =1 then 흥행수익 else 0 end) as 수익_news
					  , avg(case when 장르_lifestyle  =1 then 흥행수익 else 0 end) as 수익_lifestyle
					  , avg(case when 장르_talkshow   =1 then 흥행수익 else 0 end) as 수익_talkshow
					  , avg(case when 장르_film-noir  =1 then 흥행수익 else 0 end) as 수익_film-noir
					  , avg(case when 장르_unknown    =1 then 흥행수익 else 0 end) as 수익_unknown
				from MOVIE a1
				left join (
				select
						영화ID
					   , max(case when 장르ID = '1'           then 1 else 0 end) as 장르_history
					   , max(case when 장르ID = '2'           then 1 else 0 end) as 장르_enimation
					   , max(case when 장르ID = '3'           then 1 else 0 end) as 장르_drama
					   , max(case when 장르ID = '6'           then 1 else 0 end) as 장르_comedy
					   , max(case when 장르ID = '7'           then 1 else 0 end) as 장르_action
					   , max(case when 장르ID = '8'           then 1 else 0 end) as 장르_crime
					   , max(case when 장르ID = '9'           then 1 else 0 end) as 장르_thriller
					   , max(case when 장르ID = '10'          then 1 else 0 end) as 장르_documentary
					   , max(case when 장르ID = '11'          then 1 else 0 end) as 장르_adventure
					   , max(case when 장르ID = '12'          then 1 else 0 end) as 장르_fantasy
					   , max(case when 장르ID = '14'          then 1 else 0 end) as 장르_family
					   , max(case when 장르ID = '15'          then 1 else 0 end) as 장르_romance
					   , max(case when 장르ID = '16'          then 1 else 0 end) as 장르_music
					   , max(case when 장르ID = '17'          then 1 else 0 end) as 장르_horror
					   , max(case when 장르ID = '18'          then 1 else 0 end) as 장르_war
					   , max(case when 장르ID = '19'          then 1 else 0 end) as 장르_western
					   , max(case when 장르ID = '20'          then 1 else 0 end) as 장르_mystery
					   , max(case when 장르ID = '24'          then 1 else 0 end) as 장르_short
					   , max(case when 장르ID = '25'          then 1 else 0 end) as 장르_musical
					   , max(case when 장르ID = '30'          then 1 else 0 end) as 장르_sport
					   , max(case when 장르ID in ('43', '49') then 1 else 0 end) as 장르_gameshow
					   , max(case when 장르ID in ('44', '48') then 1 else 0 end) as 장르_reality
					   , max(case when 장르ID = '45'          then 1 else 0 end) as 장르_sci-fi
					   , max(case when 장르ID in ('46', '53') then 1 else 0 end) as 장르_Biography
					   , max(case when 장르ID in ('47', '51') then 1 else 0 end) as 장르_news
					   , max(case when 장르ID = '50'          then 1 else 0 end) as 장르_lifestyle
					   , max(case when 장르ID = '52'          then 1 else 0 end) as 장르_talkshow
					   , max(case when 장르ID = '56'          then 1 else 0 end) as 장르_film-noir
					   , max(case when 장르ID = '-1'          then 1 else 0 end) as 장르_unknown
				from MOVIE_GENRE
				group by 영화ID
			)a2
			on
				a1.영화ID = a2.영화ID
"""                                                          
, conn
)
a

# 평균 비용
a = pd.read_sql(
                """
                select
                        avg(case when 장르_history    =1 then 비용 else 0 end) as 비용_history
                      , avg(case when 장르_enimation =1 then 비용 else 0 end) as 비용_enimation
                      , avg(case when 장르_drama      =1 then 비용 else 0 end) as 비용_drama
                      , avg(case when 장르_comedy  =1 then 비용 else 0 end) as 비용_comedy
                      , avg(case when 장르_action  =1 then 비용 else 0 end) as 비용_action
                      , avg(case when 장르_crime     =1 then 비용 else 0 end) as 비용_crime
                      , avg(case when 장르_thriller   =1 then 비용 else 0 end) as 비용_thriller
                      , avg(case when 장르_documentary  =1 then 비용 else 0 end) as 비용_documenta
                      , avg(case when 장르_adventure =1 then 비용 else 0 end) as 비용_adventure
                      , avg(case when 장르_fantasy  =1 then 비용 else 0 end) as 비용_fantasy
                      , avg(case when 장르_family  =1 then 비용 else 0 end) as 비용_family
                      , avg(case when 장르_romance    =1 then 비용 else 0 end) as 비용_romance
                      , avg(case when 장르_music      =1 then 비용 else 0 end) as 비용_music
                      , avg(case when 장르_horror  =1 then 비용 else 0 end) as 비용_horror
                      , avg(case when 장르_war      =1 then 비용 else 0 end) as 비용_war
                      , avg(case when 장르_western  =1 then 비용 else 0 end) as 비용_western
                      , avg(case when 장르_mystery  =1 then 비용 else 0 end) as 비용_mystery
                      , avg(case when 장르_short      =1 then 비용 else 0 end) as 비용_short
                      , avg(case when 장르_musical  =1 then 비용 else 0 end) as 비용_musical
                      , avg(case when 장르_sport      =1 then 비용 else 0 end) as 비용_sport
                      , avg(case when 장르_gameshow  =1 then 비용 else 0 end) as 비용_gameshow
                      , avg(case when 장르_reality  =1 then 비용 else 0 end) as 비용_reality
                      , avg(case when 장르_sci_fi     =1 then 비용 else 0 end) as 비용_sci_fi
                      , avg(case when 장르_Biography  =1 then 비용 else 0 end) as 비용_Biography
                      , avg(case when 장르_news       =1 then 비용 else 0 end) as 비용_news
                      , avg(case when 장르_lifestyle  =1 then 비용 else 0 end) as 비용_lifestyle
                      , avg(case when 장르_talkshow   =1 then 비용 else 0 end) as 비용_talkshow
                      , avg(case when 장르_film_noir  =1 then 비용 else 0 end) as 비용_film_noir
                      , avg(case when 장르_unknown    =1 then 비용 else 0 end) as 비용_unknown
                from MOVIE a1
                left join (
                select
                        영화ID
                       , 장르ID 
                       , max(case when 장르ID = '1'           then 1 else 0 end) as 장르_history
                       , max(case when 장르ID = '2'           then 1 else 0 end) as 장르_enimation
                       , max(case when 장르ID = '3'           then 1 else 0 end) as 장르_drama
                       , max(case when 장르ID = '6'           then 1 else 0 end) as 장르_comedy
                       , max(case when 장르ID = '7'           then 1 else 0 end) as 장르_action
                       , max(case when 장르ID = '8'           then 1 else 0 end) as 장르_crime
                       , max(case when 장르ID = '9'           then 1 else 0 end) as 장르_thriller
                       , max(case when 장르ID = '10'          then 1 else 0 end) as 장르_documentary
                       , max(case when 장르ID = '11'          then 1 else 0 end) as 장르_adventure
                       , max(case when 장르ID = '12'          then 1 else 0 end) as 장르_fantasy
                       , max(case when 장르ID = '14'          then 1 else 0 end) as 장르_family
                      , max(case when 장르ID = '15'          then 1 else 0 end) as 장르_romance
                       , max(case when 장르ID = '16'          then 1 else 0 end) as 장르_music
                       , max(case when 장르ID = '17'          then 1 else 0 end) as 장르_horror
                      , max(case when 장르ID = '18'          then 1 else 0 end) as 장르_war
                      , max(case when 장르ID = '19'          then 1 else 0 end) as 장르_western
                       , max(case when 장르ID = '20'          then 1 else 0 end) as 장르_mystery
                      , max(case when 장르ID = '24'          then 1 else 0 end) as 장르_short
                       , max(case when 장르ID = '25'          then 1 else 0 end) as 장르_musical
                       , max(case when 장르ID = '30'          then 1 else 0 end) as 장르_sport
                       , max(case when 장르ID in ('43', '49') then 1 else 0 end) as 장르_gameshow
                       , max(case when 장르ID in ('44', '48') then 1 else 0 end) as 장르_reality
                       , max(case when 장르ID = '45'          then 1 else 0 end) as 장르_sci_fi
                       , max(case when 장르ID in ('46', '53') then 1 else 0 end) as 장르_Biography
                       , max(case when 장르ID in ('47', '51') then 1 else 0 end) as 장르_news
                       , max(case when 장르ID = '50'          then 1 else 0 end) as 장르_lifestyle
                       , max(case when 장르ID = '52'          then 1 else 0 end) as 장르_talkshow
                       , max(case when 장르ID = '56'          then 1 else 0 end) as 장르_film_noir
                       , max(case when 장르ID = '-1'          then 1 else 0 end) as 장르_unknown
                from MOVIE_GENRE
                group by 영화ID
            )a2
            on
                a1.영화ID = a2.영화ID
"""                                                          
, conn
)
a

#2.2.4 장르별 추천수
a = pd.read_sql(
            """
            select
              장르ID
            , count(a1.추천여부) as 추천수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                group by 장르ID
                order by 추천수 desc
                
 ;
 """
 , conn
)
a

################################
# 3. 활동 현황 탐색 및 이벤트 정의
################################
# 행동별 전체 현황
# 활동 패턴별 월별 현황(전체)
a = pd.read_sql(
            """
            select
                sum(a1.영화평가)
                , sum(a1.영화시청완료)
                , sum(a1.영화검색)
                , sum(a1.영화시청시작)
                , sum(a1.영화구매)
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 영화평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 영화시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 영화시청시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 영화검색
                    , max(case when 활동ID = '11' then 1 else 0 end) as 영화구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                    
                
 ;
 """
 , conn
)
a

#행동별 월별 현황(where절 변경하여 활용)
a = pd.read_sql(
            """
            select
                  기준년월
                , count(distinct 고객ID) as 고객수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 영화평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 영화시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 영화시청시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 영화검색
                    , max(case when 활동ID = '11' then 1 else 0 end) as 영화구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                    where a1.영화평가 = 1
					group by 1
                
 ;
 """
 , conn
)
a

#활동 패턴별
# 활동 패턴별 월별 현황(전체)
a = pd.read_sql(
            """
            select
                count(distinct 고객ID) as 고객수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 영화평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 영화시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 영화시청시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 영화검색
                    , max(case when 활동ID = '11' then 1 else 0 end) as 영화구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                    where 장르ID = '45'
                        and 영화구매=1 and 영화시청시작=1 and 영화시청완료=1
                
 ;
 """
 , conn
)
a

# 활동별 월별 현황
a = pd.read_sql(
            """
            select
				기준년월
                , count(distinct 고객ID) as 고객수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 영화평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 영화시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 영화시청시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 영화검색
                    , max(case when 활동ID = '11' then 1 else 0 end) as 영화구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                    where a1.영화평가
					group by 1
                
 ;
 """
 , conn
)
a

################################
# 4. 이벤트 현황
################################
#4.1 이벤트 전체 고객 수
a = pd.read_sql(
            """
            select
                 count(distinct a1.고객ID) as 고객수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 영화평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 영화시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 영화시청시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 영화검색
                    , max(case when 활동ID = '11' then 1 else 0 end) as 영화구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                    where a2.장르ID= '45' 
                        and a1.영화구매 = 1 
                        and a1.영화시청시작 = 1 
                        and a1.영화시청완료 = 1 
                        and 기준년월 != '201210'
                
 ;
 """
 , conn
)
a

#4.2 월별 이벤트 비율
a = pd.read_sql(
            """
            select
                  a1.기준년월
                , count(distinct a1.고객ID) as 고객수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 영화평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 영화시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 영화시청시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 영화검색
                    , max(case when 활동ID = '11' then 1 else 0 end) as 영화구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                    where a2.장르ID= '45' 
                        and a1.영화구매 = 1 
                        and a1.영화시청시작 = 1 
                        and a1.영화시청완료 = 1 
                        and 기준년월 != '201210'
                    group by 1
                    order by 기준년월 desc
                
 ;
 """
 , conn
)
a

#연령대와 스릴러 연관성
a = pd.read_sql(
            """
            select
                c.연령대
              , count(c.연령대) 연령대_건수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                left join (
                                        select 
                                            고객ID
                                         ,  나이
                                        , case when 나이 between '16' and '19' then '10대'
                                            when 나이 between '20' and '29' then '20대'
                                            when 나이 between '30' and '39' then '30대'
                                            when 나이 between '40' and '49' then '40대'
                                            when 나이 between '50' and '59' then '50대'
                                            when 나이 between '60' and '69' then '60대'
                                            when 나이 between '70' and '79' then '70대'
                                            when 나이 between '80' and '89' then '80대'
                                            else 나이
                                          end as 연령대
                                        from CUSTOMER 
                                        ) c 
                                        on a1.고객ID = c.고객ID
                where a1.훑어봄 = 1 
                    and a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                group by 연령대
                
                
 ;
 """
 , conn
)
a

################################
# 5. 알파(사용x, 코드 수정 후 사용 계획)
################################
# 연령대와 장르 연관성(1)
a = pd.read_sql(
            """
            select
                c.연령대
              , count(distinct a1.고객ID) 연령대_건수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                left join (
                                        select 
                                            고객ID
                                         ,  나이
                                        , case when 나이 between '16' and '19' then '10대'
                                            when 나이 between '20' and '29' then '20대'
                                            when 나이 between '30' and '39' then '30대'
                                            when 나이 between '40' and '49' then '40대'
                                            when 나이 between '50' and '59' then '50대'
                                            when 나이 between '60' and '69' then '60대'
                                            when 나이 between '70' and '79' then '70대'
                                            when 나이 between '80' and '89' then '80대'
                                            else 나이
                                          end as 연령대
                                        from CUSTOMER 
                                        ) c 
                                        on a1.고객ID = c.고객ID
                where a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                group by 연령대
                
                
 ;
 """
 , conn
)
a

# 연령대와 장르 연관성(2)
a = pd.read_sql(
            """
            select
                c.연령대
              , count(distinct a1.고객ID) 연령대_건수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                left join (
                                        select 
                                            고객ID
                                         ,  나이
                                        , case when 나이 between '16' and '19' then '10대'
                                            when 나이 between '20' and '29' then '20대'
                                            when 나이 between '30' and '39' then '30대'
                                            when 나이 between '40' and '49' then '40대'
                                            when 나이 between '50' and '59' then '50대'
                                            when 나이 between '60' and '69' then '60대'
                                            when 나이 between '70' and '79' then '70대'
                                            when 나이 between '80' and '89' then '80대'
                                            else 나이
                                          end as 연령대
                                        from CUSTOMER 
                                        ) c 
                                        on a1.고객ID = c.고객ID
                where a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                    and a2.장르ID = '45'
                group by 연령대
                
                
 ;
 """
 , conn
)
a

#대륙과 장르 연관성 (1, 대륙별 사용자 수 구하기)
a = pd.read_sql(
            """
            select
                c.대륙
              , count(distinct a1.고객ID) 대륙_건수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                    left join CUSTOMER c 
                                        on a1.고객ID = c.고객ID
                where a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                    and a2.장르ID='45'
                group by c.대륙
                
                
 ;
 """
 , conn
)
a

#대륙과 장르 연관성 (2)
a = pd.read_sql(
            """
            select
                c.대륙
              , count(distinct a1.고객ID) 대륙별고객수
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                    left join CUSTOMER c 
                                        on a1.고객ID = c.고객ID
                where a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                group by c.대륙
                
                
 ;
 """
 , conn
)
a

#결혼여부와 공상과학 연관성 (1)
a = pd.read_sql(
            """
            select
                c.결혼여부
              , count(distinct a1.고객ID) 
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                    left join CUSTOMER c 
                                        on a1.고객ID = c.고객ID
                where a1.훑어봄 = 1 
                    and a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                group by c.결혼여부
                
                
 ;
 """
 , conn
)
a

#결혼여부와 공상과학 연관성 (2)
a = pd.read_sql(
            """
            select
                c.결혼여부
              , count(distinct a1.고객ID) 
            from (
                    select
                    기준년월
                    , 고객ID
                    , 영화ID
                    , max(case when 추천여부 = '1' then 1 else 0 end) as 추천여부
                    , max(case when 활동ID = '1' then 1 else 0 end) as 평가
                    , max(case when 활동ID = '2' then 1 else 0 end) as 시청완료
                    , max(case when 활동ID = '4' then 1 else 0 end) as 시작
                    , max(case when 활동ID = '5' then 1 else 0 end) as 훑어봄
                    , max(case when 활동ID = '11' then 1 else 0 end) as 구매
                    , max(case when 순위 is null then 0 else 순위 end) as 순위
                    , sum(case when sales is null then 0 else sales end) as sales
                    from MOVIE_FACT
                    group by
                    기준년월
                    , 고객ID
                    , 영화ID
                ) a1
                    left join (
                                select
                                영화ID
                                , 장르ID
                                , max(case when 장르ID = '01' then 1 else 0 end) as genre_history
                                , max(case when 장르ID = '02' then 1 else 0 end) as genre_animation
                                , max(case when 장르ID = '03' then 1 else 0 end) as genre_drama
                                , max(case when 장르ID = '06' then 1 else 0 end) as genre_comedy
                                , max(case when 장르ID = '07' then 1 else 0 end) as genre_action
                                , max(case when 장르ID = '08' then 1 else 0 end) as genre_crime
                                , max(case when 장르ID = '09' then 1 else 0 end) as genre_thriller
                                , max(case when 장르ID = '10' then 1 else 0 end) as genre_documentary
                                , max(case when 장르ID = '11' then 1 else 0 end) as genre_adventure
                                , max(case when 장르ID = '12' then 1 else 0 end) as genre_fantasy
                                , max(case when 장르ID = '14' then 1 else 0 end) as genre_family
                                , max(case when 장르ID = '15' then 1 else 0 end) as genre_romance
                                , max(case when 장르ID = '16' then 1 else 0 end) as genre_music
                                , max(case when 장르ID = '17' then 1 else 0 end) as genre_horror
                                , max(case when 장르ID = '18' then 1 else 0 end) as genre_war
                                , max(case when 장르ID = '19' then 1 else 0 end) as genre_western
                                , max(case when 장르ID = '20' then 1 else 0 end) as genre_mystery
                                , max(case when 장르ID = '24' then 1 else 0 end) as genre_short
                                , max(case when 장르ID = '25' then 1 else 0 end) as genre_musical
                                , max(case when 장르ID = '30' then 1 else 0 end) as genre_sport
                                , max(case when 장르ID in ('43', '49') then 1 else 0 end) as genre_gameshow
                                , max(case when 장르ID in ('44', '48') then 1 else 0 end) as genre_reality
                                , max(case when 장르ID = '45' then 1 else 0 end) as genre_scifi
                                , max(case when 장르ID in ('46', '53') then 1 else 0 end) as genre_biography
                                , max(case when 장르ID in ('47', '51') then 1 else 0 end) as genre_news
                                , max(case when 장르ID = '50' then 1 else 0 end) as genre_lifestyle
                                , max(case when 장르ID = '52' then 1 else 0 end) as genre_talkshow
                                , max(case when 장르ID = '56' then 1 else 0 end) as genre_noir
                                , max(case when 장르ID = '-1' then 1 else 0 end) as genre_unknown
                                from MOVIE_GENRE
                                group by 영화ID
                                ) a2
                                on
                                a1.영화ID = a2.영화ID
                                    left join CUSTOMER c 
                                        on a1.고객ID = c.고객ID
                where a1.훑어봄 = 1 
                    and a1.시작 = 1
                    and a1.시청완료 = 1
                    and a1.구매 = 1
                    and a2.장르ID = '45'
                group by c.결혼여부
                
                
 ;
 """
 , conn
)
a