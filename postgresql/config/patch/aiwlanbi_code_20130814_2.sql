alter table rpt_scan_day add other_errors numeric;
alter table rpt_normal_request_day add other_errors numeric;
COMMENT ON COLUMN temp_login_request_success.user_domain IS '上线方式';


CREATE OR REPLACE FUNCTION etl_monitor_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_monitor_log_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert TEMP_FIXED_PARAM_ATTACK start',0);
	---------------分布式固定参数攻击表，数据来源于MONITOR日志，表结构也与之相同,80%的数据占比
	truncate table TEMP_FIXED_PARAM_ATTACK;
	insert into TEMP_FIXED_PARAM_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type,  DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and ac_ip is not null 
		and exists(select 1 from (
		select user_ip,	ac_ip
		from src_MONITOR_LOG
		where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and user_ip is not null and ac_ip is not null
		group by user_ip, ac_ip 
		having count(1)>1000
	) b where a.user_ip=b.user_ip and a.ac_ip = b.ac_ip);

	perform write_runlog(v_func_name,'insert temp_monitor_log_1 start',0);

	truncate table temp_monitor_log_1;
	insert into TEMP_MONITOR_LOG_1(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and ac_ip is not null
		and not exists(select * from (
		select user_ip,	ac_ip
		from TEMP_FIXED_PARAM_ATTACK
		where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
		group by user_ip, ac_ip)b where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.ac_ip=b.ac_ip and a.user_ip=b.user_ip);	

	perform write_runlog(v_func_name,'insert TEMP_IP_ATTACK start',0);
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
	insert into TEMP_ACCOUNT_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_2 a
	where  exists(select 1 from (
		select user_name
		from TEMP_MONITOR_LOG_2
		where detail_info='用户密码错误'
		group by user_name
		having count(1)>50)T1 where t1.user_name = a.user_name);

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
	insert into TEMP_LOGIN_REQUEST_SUCCESS(date_time,user_name,user_domain,user_agent)
	select date_time,case when position('@' in user_name)>1 then substring(user_name,1,position('@' in user_name)-1) else user_name end as user_name,
		case when position('@' in user_name)>1 then substring(user_name,position('@' in user_name)+1) else null end as user_domain
		,user_agent
	from src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
	and detail_info='认证成功' and ac_ip is not null;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_online_user_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_online_user_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete RPT_ONLINE_USER_DAY start',0);
	--------上线用户IP数日结果统计
	delete from RPT_ONLINE_USER_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert RPT_ONLINE_USER_DAY start',0);
	insert into RPT_ONLINE_USER_DAY(odate, prov_id, USER_IP_NUM)
	select odate, prov_id, count(1) USER_IP_NUM
	from TEMP_ONLINE_USER_IP where odate = to_date(vi_dealdate, 'yyyy-mm-dd') and prov_id<>-1
	group by odate, prov_id;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_online_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_online_user_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete RPT_ONLINE_USER_MONTH start',0);
	--------上线用户IP数月结果统计
	delete from RPT_ONLINE_USER_MONTH where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm');

	perform write_runlog(v_func_name,'insert RPT_ONLINE_USER_MONTH start',0);
	insert into RPT_ONLINE_USER_MONTH(odate, prov_id, USER_IP_NUM)
	select date_trunc('month', odate) as odate_1, prov_id, count(distinct user_ip) USER_IP_NUM
	from TEMP_ONLINE_USER_IP
	where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm') and prov_id<>-1
	group by odate_1, prov_id;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_status_err_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_status_err_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_status_err_day start',0);
	delete from rpt_status_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_status_err_day start',0);
	--用户状态错误分析,从订阅表得来
	insert into rpt_status_err_day(odate, prov_id, default_lock_num, flow_lock_num, cancel_lock_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,bms_node_id as prov_id,sum(case when BMS_SUBSCRIPTION_STATUS=3 then 1 else 0 end) as default_lock_num,
		sum(case when BMS_SUBSCRIPTION_STATUS=1 then 1 else 0 end) as flow_lock_num,
		sum(case when BMS_SUBSCRIPTION_STATUS=2 then 1 else 0 end) as cancel_lock_num
	from src_subscription s,temp_normal_login_request t,sys_prov_acip_info p
	where BMS_SUBSCRIPTION_STATUS in (1,2,3) and bms_create_time between to_date(vi_dealdate, 'yyyy-mm-dd') and to_date(vi_dealdate, 'yyyy-mm-dd') + 1
		and s.bms_user_name=t.user_name and t.detail_info='用户状态错误' and p.ac_prov_id=s.bms_node_id and t.ac_ip=p.ac_ip
	group by bms_node_id;
	perform write_runlog(v_func_name,'function end',0);
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;





