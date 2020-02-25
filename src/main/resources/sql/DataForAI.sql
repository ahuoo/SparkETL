select
      gameId,updateDt
     ,date,competitionName,team1,team2,currentScore1,currentScore2,offsetTime
     ,first1,second1,third1,forth1
     ,first2,second2,third2,forth2
     ,betScore,lastScore,gender,overTime1,overTime2,lastThird1,lastForth1,lastThird2,lastForth2
     ,case
         when competitionName in  ('NCAAB','NCAAB Extra Games','国家邀请赛') then 2
         when competitionName in  ('NBA季前赛','中国男子职业联赛','NBA','中国全国男子篮球联赛','菲律宾PBA总督杯','NBA G联赛') then 3
         else 1
      end as gameType
FROM
(

  SELECT min(updateDt) over(partition by date,team1,team2,lastScore order by id asc) newId
     ,min(offsetTime) over(partition by date,team1,team2,lastScore order by id asc) minOffsetTime
     ,min(updateDt) over(partition by date,team1,team2,lastScore order by id asc) gameId
	 ,updateDt, date,competitionName,team1,team2,currentScore1,currentScore2,offsetTime
    ,first1,second1
    ,case when offsetTime<=600 then (currentScore1-first1-second1) else thrid1 end as third1
    ,case when offsetTime>600 then (currentScore1-first1-second1-thrid1) else 0 end as forth1
    ,first2,second2
    ,case when offsetTime<=600 then (currentScore2-first2-second2) else thrid2 end as third2
    ,case when offsetTime>600 then (currentScore2-first2-second2-thrid2) else 0 end as forth2
    ,betScore,lastScore,if(team1 LIKE '%女子%' OR team1 LIKE '%Women%',1,0) as gender
    ,lastScore1-first1-second1-thrid1-forth1 as overTime1, lastScore2-first2-second2-thrid2-forth2 as overTime2
    ,thrid1 as lastThird1,forth1 as lastForth1,thrid2 as lastThird2,forth2 as lastForth2
    FROM t_raw_data
    WHERE betScore>0
 )t
 where t.minOffsetTime>=0

