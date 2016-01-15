DROP EXTERNAL TABLE ext_cboss_log;
CREATE EXTERNAL TABLE ext_cboss_log
(
  part1 ,
  part2 ,
  part3 ,
  part4 ,
  part5 ,
  part6 ,
  part7 ,
  part8 ,
  part9 ,
  part10 ,
  part11 ,
  part12 ,
  part13 ,
  part14 ,
  part15 ,
  part16 ,
  part17 ,
  part18 ,
  part19 ,
  part20 ,
  part21 
)
 LOCATION (
    'gpfdist://10.3.3.138:8004/monitor*'
)
 FORMAT 'text' (delimiter '{' null '\\N' escape '\\' fill missing fields)
ENCODING 'GBK'
LOG ERRORS INTO err_ext_cboss_log SEGMENT REJECT LIMIT 200000000 ROWS;

CREATE OR REPLACE FUNCTION etl_rpt_online_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	--------上线用户IP数月结果统计
	delete from RPT_ONLINE_USER_MONTH where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm');
	
	insert into RPT_ONLINE_USER_MONTH(odate, prov_id, USER_IP_NUM)
	select date_trunc('month', odate) as odate_1, prov_id, count(1) USER_IP_NUM
	from TEMP_ONLINE_USER_IP
	where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm')
	group by odate_1, prov_id;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;