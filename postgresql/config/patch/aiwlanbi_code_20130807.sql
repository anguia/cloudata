CREATE OR REPLACE FUNCTION check_ip(vi_ip text)
  RETURNS inet AS
$BODY$
declare
	is_ip boolean;
	result inet;
begin

	is_ip = (select vi_ip ~ '^((25[0-5])|(2[0-4]\\d)|(1\\d\\d)|([1-9]\\d)|\\d)(.((25[0-5])|(2[0-4]\\d)|(1\\d\\d)|([1-9]\\d)|\\d)){3}$');

	if is_ip = 't' then
		result = vi_ip :: inet;
	end if;

	return result;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_status_err_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	delete from rpt_status_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--用户状态错误分析,从订阅表得来
	insert into rpt_status_err_day(odate, prov_id, default_lock_num, flow_lock_num, cancel_lock_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,bms_node_id as prov_id,sum(case when BMS_SUBSCRIPTION_STATUS=3 then 1 else 0 end) as default_lock_num,
		sum(case when BMS_SUBSCRIPTION_STATUS=1 then 1 else 0 end) as flow_lock_num,
		sum(case when BMS_SUBSCRIPTION_STATUS=2 then 1 else 0 end) as cancel_lock_num
	from src_subscription
	where BMS_SUBSCRIPTION_STATUS in (1,2,3) and bms_create_time between to_date(vi_dealdate, 'yyyy-mm-dd') and to_date(vi_dealdate, 'yyyy-mm-dd') + 1
	group by bms_node_id;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;


COMMENT ON TABLE temp_login_request_success
  IS '登入认证请求“webauth_logon”，结果状态为“认证成功”的临时表，用于话单关联。  注意，不用去重';
COMMENT ON COLUMN temp_login_request_success.date_time IS '日期时间';
COMMENT ON COLUMN temp_login_request_success.user_name IS '用户名称';
COMMENT ON COLUMN temp_login_request_success.user_domain IS '登录类型';
COMMENT ON COLUMN temp_login_request_success.user_agent IS '用户UA';

CREATE TABLE temp_src_monitor_log
(
  date_time timestamp without time zone,
  user_name character(64),
  user_ip text,
  ac_ip text,
  op_type character(100),
  stype text,
  err_type text,
  detail_info character(100),
  user_agent text
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (date_time, user_ip)
;

CREATE TABLE temp_monitor_userip
(
  user_ip text,
  inet_ip inet
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (user_ip);

CREATE TABLE temp_monitor_acip
(
  ac_ip text,
  inet_ip inet
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (ac_ip);

CREATE OR REPLACE FUNCTION etl_monitor_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
begin
	--判断是否存在对应的分区，没有则增加
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if not exists(select 1 from pg_partitions where lower(tablename)=lower('src_MONITOR_LOG') and partitionname = v_partition_name) then
		execute ' alter table src_MONITOR_LOG add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	--从外部表抽取数据到src原始数据表(临时表)，注意acip为空的不需要抽取
	truncate table temp_src_MONITOR_LOG;
	insert into temp_src_MONITOR_LOG(DATE_TIME, USER_NAME, USER_IP, AC_IP, op_type, stype, err_type, DETAIL_INFO, USER_AGENT)
	select * from (
		select DATE_TIME, USER_NAME, user_ip,ac_ip, op_type, stype , err_type, DETAIL_INFO, USER_AGENT
		from (
			select to_timestamp(p_date ||' '||p_time, 'yyyy/mm/dd hh24:mi:ss') as DATE_TIME, trim(both '{|}' from USER_NAME) as USER_NAME
					,trim(both '{|}' from USER_ip) as USER_ip 
					,trim(both '{|}' from ac_ip) as ac_ip 
					,trim(both '{|}' from op_type) as op_type
					,trim(both '{|}' from stype) as stype 
					,trim(both '{|}' from err_type) as err_type 
					,trim(both '{|}' from err_detail) as DETAIL_INFO 
					,trim(both '{|}' from USER_AGENT) as USER_AGENT 
			from EXT_MONITOR_LOG
		) abc 
	) tmp 
	where  date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and ac_ip is not null;

	truncate table temp_monitor_userip;
	insert into temp_monitor_userip(user_ip,inet_ip)
	select t.user_ip,check_ip(t.user_ip) as inet_ip 
	from temp_src_MONITOR_LOG t group by user_ip;

	truncate table temp_monitor_acip;
	insert into temp_monitor_acip(ac_ip,inet_ip)
	select t.ac_ip,check_ip(t.ac_ip) as inet_ip 
	from temp_src_MONITOR_LOG t group by ac_ip;

	insert into src_MONITOR_LOG(DATE_TIME, USER_NAME, USER_IP, AC_IP, op_type, stype, err_type, DETAIL_INFO, USER_AGENT)
	select s.DATE_TIME, s.USER_NAME, u.inet_ip as USER_IP, a.inet_ip as AC_IP, s.op_type, s.stype, s.err_type, s.DETAIL_INFO, s.USER_AGENT
	from temp_src_MONITOR_LOG s 
		left join temp_monitor_userip u on s.user_ip=u.user_ip 
		left join temp_monitor_acip a on s.ac_ip=a.ac_ip;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_apache_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
begin
	-------分时间段调用， APACHE日志入库。

	truncate table temp_APACHE_LOG;
	------------外部表提前入临时表
	insert into temp_APACHE_LOG(ODATE, USER_IP, PAGE_TYPE, STATUS_CODE, p_count)
	select ODATE, user_ip
		, case when c.page_id is null then -1 else c.page_id end as page_type, status_code, count(1)
	from (
		select trim(arr_1[1]):: inet as USER_IP, to_date(trim(arr_1[2]), 'dd/mon/yyyy:hh24:mi:ss +ms') as ODATE, 
		case when position(' 200 ' in part2) >0 then 200 else -1 end as status_code, part2
			from (
				select string_to_array(part1, ' - - [') as arr_1, 
					part2 
				from EXT_APACHE_LOG
		) tmp
	) a
	left join sys_page_config c on position(c.page_url in a.part2) > 0
	where ODATE = to_date(vi_dealDate, 'yyyy-mm-dd')
	group by user_ip, ODATE, page_type, status_code;

	---------临时表出来， 计算IP属于那个省份。
	truncate table temp_userip_prov;
	insert into temp_userip_prov(user_ip, prov_id)
	select user_ip, case when b.prov_id is null then -1 else b.prov_id end as prov_id 
	from(
		select user_ip
		from temp_APACHE_LOG
		group by user_ip
	) a
	left join SYS_prov_ipseg_info b on  a.user_ip between b.start_ip and b.end_ip;

	---------外部表入库， 入中间表
	insert into SRC_APACHE_LOG(ODATE, PROV_ID, USER_IP, PAGE_TYPE, STATUS_CODE, p_count)
	select ODATE, prov_id, a.USER_IP, PAGE_TYPE, STATUS_CODE, p_count
	from temp_APACHE_LOG a
	left join temp_userip_prov b on a.user_ip = b.user_ip
	where b.prov_id != -1;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;



CREATE TABLE temp_src_radius_log
(
  date_time timestamp without time zone, -- 日期时间
  user_name character(128), -- 用户名称
  nas_ip text, -- AC IP
  mac text, -- MAC地址
  result text, -- 请求结果
  result_type character varying(50), -- 结果类型
  authen_type character varying(20)
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (date_time, nas_ip)
PARTITION BY RANGE(date_time) 
          (
          PARTITION prt_20130422 START ('2013-04-22 00:00:00'::timestamp without time zone) END ('2013-04-23 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresstype=zlib, compresslevel=5), 
          PARTITION prt_20130601 START ('2013-06-01 00:00:00'::timestamp without time zone) END ('2013-06-02 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          PARTITION prt_20130617 START ('2013-06-17 00:00:00'::timestamp without time zone) END ('2013-06-18 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          PARTITION prt_20130618 START ('2013-06-18 00:00:00'::timestamp without time zone) END ('2013-06-19 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          PARTITION prt_20130619 START ('2013-06-19 00:00:00'::timestamp without time zone) END ('2013-06-20 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          PARTITION prt_20130720 START ('2013-07-20 00:00:00'::timestamp without time zone) END ('2013-07-21 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          PARTITION prt_2013071 START ('2013-07-22 00:00:00'::timestamp without time zone) END ('2013-07-23 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib)
          )
;
ALTER TABLE temp_src_radius_log
  OWNER TO aidns;
COMMENT ON TABLE temp_src_radius_log
  IS '从RADIUS原始数据得到的有价值数据，注意取数据过程需要过滤掉计费数据，只需要访问请求数据，条件为part1 like ''%Access-Request:%''';
COMMENT ON COLUMN temp_src_radius_log.date_time IS '日期时间';
COMMENT ON COLUMN temp_src_radius_log.user_name IS '用户名称';
COMMENT ON COLUMN temp_src_radius_log.nas_ip IS 'AC IP';
COMMENT ON COLUMN temp_src_radius_log.mac IS 'MAC地址';
COMMENT ON COLUMN temp_src_radius_log.result IS '请求结果';
COMMENT ON COLUMN temp_src_radius_log.result_type IS '结果类型';

CREATE TABLE temp_src_radius_log_nas_ip
(
  nas_ip_txt text,
  nas_ip inet
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (nas_ip_txt);

CREATE OR REPLACE FUNCTION etl_radius_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
begin
	--------------- RADIUS 执行表
	--判断是否存在抽取当天分区，没有则增加
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if not exists(select 1 from pg_partitions where lower(tablename)=lower('SRC_RADIUS_LOG') and partitionname = v_partition_name) then
		execute ' alter table SRC_RADIUS_LOG add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	truncate table temp_src_radius_log;
	
	--从外部表导入数据到temp_src_radius_log表
	insert into temp_src_radius_log(date_time, user_name, nas_ip, MAC, result, result_type, AUTHEN_TYPE)
	select date_time, user_name, nas_ip, MAC, arr_3_1_1[1], result_type, AUTHEN_TYPE
	from(
		select date_time, user_name, nas_ip, MAC, string_to_array(arr_3_1[2], '>') as arr_3_1_1, arr_3_1[1] as result_type
			, arr_3_2[4] as AUTHEN_TYPE
 		from ( 
			select date_time,  trim(replace(trim(arr_1[3], '-'), 'Access-Request:', '')) as user_name
				,trim(arr_2[1]) as nas_ip, trim(arr_2[4])  as MAC, string_to_array(arr_3[2],';') as arr_3_1
				,string_to_array(arr_3[1],',') as arr_3_2 
			from (
				select to_timestamp(trim(substring(part1, 1, 19)), 'yyyy/mm/dd hh24:mi:ss') as date_time, string_to_array(part1, '#') as arr_1, string_to_array(part2, ',') as arr_2, string_to_array(part3, '<') as arr_3
				from EXT_RADIUS_LOG 
				where part1 like '%Access-Request:%' and  substring(part1, 1, 10) = replace(vi_dealdate, '-', '/')
			) tmp
		) a
	)b;

	truncate table temp_src_radius_log_nas_ip;
	------------IP转换，处理非法的NAS_IP
	insert into temp_src_radius_log_nas_ip(nas_ip_txt, nas_ip) 
	select nas_ip, check_ip(nas_ip)
	from (
		select nas_ip 
		from temp_src_radius_log
		group by nas_ip
	) a ;

	----------临时数据进入src_radius_log表
	insert into src_radius_log(date_time, user_name, nas_ip, MAC, result, result_type, AUTHEN_TYPE)
	select date_time, user_name, b.nas_ip, MAC, result, result_type, AUTHEN_TYPE
	from temp_src_radius_log a 
	left join temp_src_radius_log_nas_ip b on a.nas_ip = b.nas_ip_txt;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;