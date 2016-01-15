alter table RPT_INTRO_PAGE_DAY add column page_type integer;
alter table RPT_INTRO_PAGE_DAY add column p_count numeric;
drop table rpt_apache_log_add_day;
drop table TEMP_APACHE_LOG_ADD_DAY;


CREATE TABLE rpt_intro_page_month
(
  odate date, -- 日期
  prov_id integer, -- 省份标识
  user_ip inet, -- 用户IP
  page_type integer,
  p_count numeric
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (user_ip);

COMMENT ON TABLE rpt_intro_page_month IS '访问介绍页面的IP记录';
COMMENT ON COLUMN rpt_intro_page_month.odate IS '日期';
COMMENT ON COLUMN rpt_intro_page_month.prov_id IS '省份标识';
COMMENT ON COLUMN rpt_intro_page_month.user_ip IS '用户IP';
COMMENT ON COLUMN rpt_intro_page_month.page_type IS '页面类型';
COMMENT ON COLUMN rpt_intro_page_month.p_count IS '访问次数';

-- Function: etl_rpt_page_day(text)

-- DROP FUNCTION etl_rpt_page_day(text);

CREATE OR REPLACE FUNCTION etl_rpt_page_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_page_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------APCHE日志, 页面统计

	perform write_runlog(v_func_name,'delete RPT_INTRO_PAGE_DAY start',0);
	--------------是潜在用户， 并且访问了介绍页面
	delete from RPT_INTRO_PAGE_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	insert into RPT_INTRO_PAGE_DAY(odate, prov_id, page_type, user_ip, p_count)
	select odate, prov_id, page_type, a.user_ip, p_count
	from SRC_APACHE_LOG a
	left join (
		select user_ip  ---------------------得到潜在用户
		from SRC_APACHE_LOG a 
		where page_type = 1 and  a.odate = to_date(vi_dealdate, 'yyyy-mm-dd') and status_code = 200 and not exists(
			select 1 from temp_online_user_ip b where a.user_ip = b.user_ip and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		)
		group by user_ip
	) b on a.user_ip = b.user_ip
	where page_type>1 and a.odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	
	perform write_runlog(v_func_name,'delete rpt_page_day start',0);
	-----------分页统计页面， 
	delete from rpt_page_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	perform write_runlog(v_func_name,'insert rpt_page_day start',0);
	insert into rpt_page_day(odate, prov_id, PAGE_TYPE, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
		select odate, prov_id, page_type, count(1) as POTENTIAL_USER_NUM, sum(p_count) INTRO_PAGE_NUM
	from (
		select odate, prov_id, page_type, user_ip, sum(p_count) as p_count
		from RPT_INTRO_PAGE_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id, page_type, user_ip
	) a
	group by odate, prov_id, page_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
  
  
  
-- Function: etl_rpt_page_month(text)

-- DROP FUNCTION etl_rpt_page_month(text);

CREATE OR REPLACE FUNCTION etl_rpt_page_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_page_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------统计Portal访问相关信息, 月报表统计

	perform write_runlog(v_func_name,'delete rpt_intro_page_month start',0);
	delete from rpt_intro_page_month where odate = to_date(vi_dealdate, 'yyyy-mm'); 
	---------是潜在用户的IP， 页面导出潜在用户IP
	insert into rpt_intro_page_month(odate, prov_id, user_ip, page_type, p_count)
	select date_trunc('month', odate) :: date as m_odate, prov_id, user_ip, page_type, sum(p_count)
	from RPT_INTRO_PAGE_DAY t1
	where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm')
		and not exists(
			select 1 from TEMP_ONLINE_USER_IP t2 where t1.user_ip = t2.user_ip
				and date_trunc('month', t2.odate) = to_date(vi_dealdate, 'yyyy-mm')
		)
	group by m_odate, prov_id, user_ip, page_type;

	---------分页面统计,月表
	perform write_runlog(v_func_name,'insert RPT_PAGE_MONTH start',0);
	delete from RPT_PAGE_MONTH where odate = to_date(vi_dealdate, 'yyyy-mm'); 
	
	insert into RPT_PAGE_MONTH(odate, prov_id, PAGE_TYPE, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.odate, a.prov_id, a.page_type, count(1), sum(p_count)
	from rpt_intro_page_month a
	where a.odate = to_date(vi_dealdate, 'yyyy-mm')
	group by a.odate, a.prov_id, a.page_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;

