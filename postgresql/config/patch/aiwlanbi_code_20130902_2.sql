CREATE OR REPLACE FUNCTION etl_monitor_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_monitor_log_day';
begin
	perform write_runlog(v_func_name,'function start',0);

	perform write_runlog(v_func_name,'insert temp_monitor_fixparam_attack start',0);
	truncate table temp_monitor_fixparam_attack;
	insert into temp_monitor_fixparam_attack(user_ip,ac_ip)
	select user_ip,	ac_ip
		from src_MONITOR_LOG
		where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and ac_ip is not null
		group by user_ip, ac_ip 
		having count(1)>1000;
		
	perform write_runlog(v_func_name,'insert TEMP_FIXED_PARAM_ATTACK start',0);
	---------------分布式固定参数攻击表，数据来源于MONITOR日志，表结构也与之相同,80%的数据占比
	truncate table TEMP_FIXED_PARAM_ATTACK;
	insert into TEMP_FIXED_PARAM_ATTACK(DATE_TIME, USER_IP, AC_IP, stype, err_type,  DETAIL_INFO, USER_AGENT,user_type,login_type,NUM)
	select to_date(vi_dealDate, 'yyyy-mm-dd'), a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT,
		(case when user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type,
	        (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CLT.PC' in upper(user_name)) > 0 then 3
		             when position('CLT.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type,
		count(*)
	from  src_MONITOR_LOG a,temp_monitor_fixparam_attack b
	where op_type='webauth_logon' and date_trunc('day', a.DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.user_ip=b.user_ip and a.ac_ip = b.ac_ip
	group by a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT,user_type,login_type;

	perform write_runlog(v_func_name,'insert temp_monitor_log_1 start',0);

	truncate table temp_monitor_log_1;
	insert into TEMP_MONITOR_LOG_1(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and ac_ip is not null
		and not exists(select 1 from temp_monitor_fixparam_attack b where a.ac_ip=b.ac_ip and a.user_ip=b.user_ip);	

	perform write_runlog(v_func_name,'insert TEMP_IP_ATTACK start',0);
	-------------------独立IP高频次攻击表
	truncate table TEMP_IP_ATTACK;
	insert into TEMP_IP_ATTACK(DATE_TIME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT,user_type,login_type,NUM)
	select to_date(vi_dealDate, 'yyyy-mm-dd') as DATE_TIME, a.USER_IP, a.AC_IP, a.stype, err_type, a.DETAIL_INFO, a.USER_AGENT,
		(case when user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type,
	        (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CLT.PC' in upper(user_name)) > 0 then 3
		             when position('CLT.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type,
		count(*)
	from  TEMP_MONITOR_LOG_1 a 
	where exists(select * from (
		select user_ip
		from TEMP_MONITOR_LOG_1
		group by user_ip
		having count(1) > 100
	) b where  a.user_ip=b.user_ip )
	group by a.USER_IP, a.AC_IP, a.stype, err_type, a.DETAIL_INFO, a.USER_AGENT,user_type,login_type;

	perform write_runlog(v_func_name,'insert TEMP_MONITOR_LOG_2 start',0);
	truncate table TEMP_MONITOR_LOG_2;
	insert into TEMP_MONITOR_LOG_2(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_1 a
	where not exists(select * from (
		select user_ip
		from TEMP_IP_ATTACK
		group by user_ip)b where a.user_ip=b.user_ip) ;

	perform write_runlog(v_func_name,'insert TEMP_ACCOUNT_ATTACK start',0);
	----------------独立帐号高频次攻击表
	truncate table TEMP_ACCOUNT_ATTACK;
	insert into TEMP_ACCOUNT_ATTACK(DATE_TIME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT,user_type,login_type,NUM)
	select to_date(vi_dealDate, 'yyyy-mm-dd') as DATE_TIME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT,
		(case when user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type,
	        (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CLT.PC' in upper(user_name)) > 0 then 3
		             when position('CLT.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type,
		count(*)
	from  TEMP_MONITOR_LOG_2 a
	where  exists(select 1 from (
		select user_name
		from TEMP_MONITOR_LOG_2
		where detail_info='用户密码错误'
		group by user_name
		having count(1)>50)T1 where t1.user_name = a.user_name)
	group by a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT,user_type,login_type;

	perform write_runlog(v_func_name,'insert TEMP_NORMAL_LOGIN_REQUEST start',0);
	----------------正常用户上线请求表，数据来源于MONITOR日志，10%的数据占比
	truncate table TEMP_NORMAL_LOGIN_REQUEST;
	insert into TEMP_NORMAL_LOGIN_REQUEST(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_2 a
	where  not exists(select 1 from (
		select user_name
		from TEMP_ACCOUNT_ATTACK
		group by user_name)T1 where t1.user_name = a.user_name);

	perform write_runlog(v_func_name,'delete TEMP_LOGIN_REQUEST_SUCCESS start',0);
	--钻取登录成功的数据保存到TEMP_LOGIN_REQUEST_SUCCESS临时表，用于和话单关联
	--execute 'alter table TEMP_LOGIN_REQUEST_SUCCESS truncate partition '||v_partition_name;
	delete from TEMP_LOGIN_REQUEST_SUCCESS where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd');
	perform write_runlog(v_func_name,'insert TEMP_LOGIN_REQUEST_SUCCESS start',0);
	insert into TEMP_LOGIN_REQUEST_SUCCESS(date_time,user_name,user_agent)
	select date_time,case when position('@' in user_name)>1 then substring(user_name,1,position('@' in user_name)-1) else user_name end as user_name
		,user_agent
	from src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
	and detail_info='认证成功' and ac_ip is not null;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;