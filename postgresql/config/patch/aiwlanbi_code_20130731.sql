insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (113, 1, '/home/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (114, 1, '/widget/**', 'Y');

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

	--从外部表抽取数据到src原始数据表，注意acip为空的不需要抽取
	insert into src_MONITOR_LOG(DATE_TIME, USER_NAME, USER_IP, AC_IP, op_type, stype, err_type, DETAIL_INFO, USER_AGENT)
	select * from (
		select DATE_TIME, USER_NAME, (case when position('.' in user_ip)>1 and length(user_ip) > 7 
				then user_ip else null end) ::inet
			, (case when length(ac_ip)>7 then ac_ip else null end)::inet as ac_ip, op_type, stype , err_type, DETAIL_INFO, USER_AGENT
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
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_monitor_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	---------------分布式固定参数攻击表，数据来源于MONITOR日志，表结构也与之相同,80%的数据占比
	truncate table TEMP_FIXED_PARAM_ATTACK;
	insert into TEMP_FIXED_PARAM_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type,  DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and exists(select 1 from (
		select user_ip,	ac_ip
		from src_MONITOR_LOG
		where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and user_ip is not null and ac_ip is not null
		group by user_ip, ac_ip 
		having count(1)>1000
	) b where a.user_ip=b.user_ip and a.ac_ip = b.ac_ip);

	truncate table temp_monitor_log_1;
	insert into TEMP_MONITOR_LOG_1(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and not exists(select * from (
		select user_ip,	ac_ip
		from TEMP_FIXED_PARAM_ATTACK
		where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
		group by user_ip, ac_ip)b where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.ac_ip=b.ac_ip and a.user_ip=b.user_ip);	

	-------------------独立IP高频次攻击表
	truncate table TEMP_IP_ATTACK;
	insert into TEMP_IP_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_1 a 
	where exists(select * from (
		select user_ip
		from TEMP_MONITOR_LOG_1
		group by user_ip
		having count(1) > 100
		) b where  a.user_ip=b.user_ip );
		
	truncate table TEMP_MONITOR_LOG_2;
	insert into TEMP_MONITOR_LOG_2(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_1 a
	where not exists(select * from (
		select user_ip
		from TEMP_IP_ATTACK
		group by user_ip)b where a.user_ip=b.user_ip) ;

	----------------独立帐号高频次攻击表
	truncate table TEMP_ACCOUNT_ATTACK;
	insert into TEMP_ACCOUNT_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_2 a
	where  exists(select 1 from (
		select user_name
		from TEMP_MONITOR_LOG_2
		where detail_info='用户密码错误'
		group by user_name
		having count(1)>50)T1 where t1.user_name = a.user_name);

	----------------正常用户上线请求表，数据来源于MONITOR日志，10%的数据占比
	truncate table TEMP_NORMAL_LOGIN_REQUEST;
	insert into TEMP_NORMAL_LOGIN_REQUEST(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_2 a
	where  not exists(select 1 from (
		select user_name
		from TEMP_ACCOUNT_ATTACK
		group by user_name)T1 where t1.user_name = a.user_name);

	--钻取登录成功的数据保存到TEMP_LOGIN_REQUEST_SUCCESS临时表，用于和话单关联
	--execute 'alter table TEMP_LOGIN_REQUEST_SUCCESS truncate partition '||v_partition_name;
	delete from TEMP_LOGIN_REQUEST_SUCCESS where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd');
	insert into TEMP_LOGIN_REQUEST_SUCCESS(date_time,user_name,user_agent)
	select date_time,user_name,user_agent
	from src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
	and detail_info='认证成功';
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;
