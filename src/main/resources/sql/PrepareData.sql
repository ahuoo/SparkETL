SELECT id,date,competitionName,team1,team2,currentScore1,currentScore2,offsetTime
,first1,second1,case when offsetTime<=600 then (currentScore1-first1-second1) else thrid1 end as third1_ ,case when offsetTime>600 then (currentScore1-first1-second1-thrid1) else 0 end as forth1_
,first2,second2,case when offsetTime<=600 then (currentScore2-first2-second2) else thrid2 end as third2_ ,case when offsetTime>600 then (currentScore2-first2-second2-thrid2) else 0 end as forth2_
,betScore,lastScore, if(team1 LIKE '%女子%' OR team1 LIKE '%Women%',1,0) as gender, updateDt,suspended
FROM ball.increase
WHERE betScore>0
order by id

