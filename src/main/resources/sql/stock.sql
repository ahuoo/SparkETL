with o as (
    select
        *,
        row_number() over(partition by code order by datetime asc) num
    from t_stock
    where datetime like '%93000' --and code ='000060.SZ'
    ),
    p as (
       SELECT
            o.code,
            o.date,
            concat(o.date,'160000') as datetime,
            o2.close,
            o2.vol,
            o2.amount
        FROM o join o as o2 on o.code=o2.code and o.num=o2.num-1
    )
select
  t1.code
 ,t1.date
 --,collect_list(substring(datetime,10)) as datetime
 ,collect_list(concat_ws(':',substring(datetime,9,2),substring(datetime,11,2),substring(datetime,13,2))) as datetime
 ,collect_list(close) as price
 ,collect_list(vol) as vol
 ,collect_list(amount) as amount
from(
    select * from
    (
        select s.code,s.date,s.datetime,s.close,s.vol,s.amount from t_stock s join p on s.code=p.code and s.date=p.date --where s.code ='000060.SZ'
        union all
        select code,date,datetime,close,vol,amount  from p
    )t
    order by code,datetime
)t1
group by t1.code, t1.date
order by code,date
