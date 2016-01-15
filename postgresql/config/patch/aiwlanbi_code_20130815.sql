comment on column RPT_ACTIVE_UA_TYPE_DAY.USER_TYPE is 
'用户类型，1为公众用户，2为校园用户，3为集团用户，4为预付费卡用户';

alter table TEMP_LOGIN_REQUEST_SUCCESS drop column user_domain;

DROP TABLE temp_apache_log;

CREATE TABLE temp_apache_log
(
  odate date,
  user_ip text,
  page_type integer,
  status_code integer,
  p_count integer
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, COMPRESSTYPE=zlib, 
  OIDS=FALSE
)
DISTRIBUTED BY (user_ip);

DROP TABLE temp_userip_prov;

CREATE TABLE temp_userip_prov
(
  user_ip_txt text,
  user_ip inet,
  prov_id integer
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (user_ip);



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
	from src_subscription s,(select case when position('@' in user_name)>1 then substring(user_name,1,position('@' in user_name)-1) else user_name end as user_name,
		detail_info,ac_ip from temp_normal_login_request) t,sys_prov_acip_info p
	where BMS_SUBSCRIPTION_STATUS in (1,2,3) and bms_create_time between to_date(vi_dealdate, 'yyyy-mm-dd') and to_date(vi_dealdate, 'yyyy-mm-dd') + 1
		and s.bms_user_name=t.user_name and t.detail_info='用户状态错误' and p.ac_prov_id=s.bms_node_id and t.ac_ip=p.ac_ip
	group by bms_node_id;
	perform write_runlog(v_func_name,'function end',0);
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_cha_err_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_cha_err_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_cha_err_day start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	--清理当前统计日期下的数据
	delete from rpt_cha_err_day where odate = to_date(vi_dealDate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_cha_err_day start',0);
	
	--统计“三种错误账号已在线分析”数量
	insert into rpt_cha_err_day (odate,prov_id,err_type,err_num)
	select to_date(vi_dealDate, 'yyyy-mm-dd'),b.prov_id,b.err_type,count(1) err_num
	from (
		select t.user_name
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
	) a,
	(
		select t.prov_id,t.err_type,t.user_name
		from temp_cha_err t
		where date_trunc('day', t.date_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		and t.err_type <> 4
	) b
	where a.user_name = b.user_name
	group by to_date(vi_dealDate, 'yyyy-mm-dd'),b.prov_id,b.err_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_subscription_day(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_subscription_day';
begin

	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_subscription_day start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_subscription_day where odate = to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_subscription_day start',0);
	
	--统计用户套餐订购情况
	insert into rpt_subscription_day (odate,bms_node_id,user_suite_type,user_num,new_user_num)	
	select to_date(vi_dealdate,'yyyy-mm-dd'),
	       n.prov_id,
	       n.user_suit_type,
	       COALESCE(n.user_num,0),
	       COALESCE(n.new_user_num,0)
	from (
	select prov.prov_id,d.user_suit_type,d.user_num,d.new_user_num
	from sys_prov_info prov
	left join (

	--统计公共用户包时套餐，校园用户包时套餐
	select t.bms_node_id,
	       (case when t.bms_product_id = 22 and t.bms_subscription_status = 0 then 1 
	             when t.bms_product_id = 81 and t.bms_subscription_status = 0 then 2
		     when t.bms_product_id = 82 and t.bms_subscription_status = 0 then 3
		     when t.bms_product_id = 83 and t.bms_subscription_status = 0 then 4
		     when (t.bms_product_id = 16 or t.bms_product_id = 19) and t.bms_subscription_status = 0 then 5
		     when (t.bms_product_id = 17 or t.bms_product_id = 20) and t.bms_subscription_status = 0 then 6
		     when (t.bms_product_id = 18 or t.bms_product_id = 21) and t.bms_subscription_status = 0 then 7
		     when t.bms_product_id = 84 and t.bms_subscription_status = 0 then 8
		     when (t.bms_product_id = 33 or t.bms_product_id = 40) and t.bms_subscription_status = 0 then 12
		     when (t.bms_product_id = 34 or t.bms_product_id = 41) and t.bms_subscription_status = 0 then 13
		     when (t.bms_product_id = 35 or t.bms_product_id = 42) and t.bms_subscription_status = 0 then 14
		     else 0 end) user_suit_type,count(t.bms_user_name) user_num	,
	        sum(case when date_trunc('day',t.bms_create_time) = to_timestamp(vi_dealdate,'yyyy-mm-dd') then 1 else 0 end) new_user_num
	from src_subscription t
	where t.bms_create_time <to_timestamp(v_end_date,'yyyy-mm-dd')
	and t.bms_node_id != 0	
	group by t.bms_node_id,user_suit_type

	union all

	--统计公共用户包流量套餐
	select t.bms_node_id,
	       (case when p.package_name ='10元自动认证套餐' then 9
	             when p.package_name ='20元自动认证套餐' then 10
	             when p.package_name ='50元自动认证套餐' then 11
	             when p.package_name ='包月1G' then 10
	             else 0 end) user_suit_type,count(t.bms_user_name) user_num,
	       sum(case when date_trunc('day',t.bms_create_time) = to_timestamp(vi_dealdate,'yyyy-mm-dd') then 1 else 0 end) new_user_num
	from src_subscription t,src_wlan_package p
	where p.time_stamp <to_timestamp(v_end_date,'yyyy-mm-dd') 
	and t.bms_user_name = p.bms_user_name 
	and t.bms_product_id != 32
	group by t.bms_node_id,user_suit_type
	) d on prov.prov_id = d.bms_node_id) n ;

	perform write_runlog(v_func_name,'function end',0);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION etl_rpt_pwd_err_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_pwd_err_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_pwd_err_day start',0);
	delete from rpt_pwd_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_pwd_err_day start',0);
	--根据省份，统计部分成功数和完全失败的数
	insert into rpt_pwd_err_day(odate, prov_id, part_failed_num, all_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.ac_prov_id as prov_id,
		sum(case when fail_flag=1 and success_flag=1 then 1 else 0 end) as part_failed_num,
		sum(case when fail_flag=1 and success_flag=0 then 1 else 0 end) as all_failed_num
	 from (select user_name,ac_ip,
		max(case when detail_info='用户密码错误' then 1 else 0 end) as fail_flag,
		max(case when detail_info='认证成功' then 1 else 0 end) as success_flag
	from temp_normal_login_request group by user_name,ac_ip) t,sys_prov_acip_info c  
	where t.ac_ip=c.ac_ip group by c.ac_prov_id;

	perform write_runlog(v_func_name,'function end',0);

end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

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

CREATE OR REPLACE FUNCTION etl_rpt_normal_request_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_normal_request_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_normal_request_day start',0);
	delete from rpt_normal_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_normal_request_day start',0);
	--正常用户上线请求统计
	insert into rpt_normal_request_day(odate, prov_id, user_type, login_type, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, other_errors,
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success_total as other_errors,
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,success_total
        from (select ac_ip,user_type,login_type,count(*) as total,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as starbuck_auth_rejected,sum(success) as success_total
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_matched,
		             case when detail_info='OBS访问失败' then 1 else 0 end as obs_failed,
		             case when detail_info='其他错误' and err_type='OBS_ERROR' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误' and err_type='PORTAL_ERROR' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' and err_type='AC_ERROR' then 1 else 0 end as other_failed,
		             case when detail_info='请求auth，上线BAS错误' then 1 else 0 end as auth_bas_err,
		             case when detail_info='请求Challenge，上线BAS错误' then 1 else 0 end as cha_bas_err,
		             case when detail_info='请求Challenge被拒绝' then 1 else 0 end as cha_rejected,
		             case when detail_info='请求Challenge此链接已建立' then 1 else 0 end as cha_connected,
		             case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 1 else 0 end as auth_blocked,
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info='认证成功' then 1 else 0 end as success
	from temp_normal_login_request) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_scan_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_scan_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_scan_day start',0);
	delete from rpt_scan_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_scan_day1 start',0);
	--分布式固定参数攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, other_errors,
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,1 as scan_type, total as scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success as other_errors,
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,success as success_total
        from (select ac_ip,user_type,login_type,count(*) as total,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected,sum(success) as success
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_matched,
		             case when detail_info='OBS访问失败' then 1 else 0 end as obs_failed,
		             case when detail_info='其他错误' and err_type='OBS_ERROR' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误' and err_type='PORTAL_ERROR' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' and err_type='AC_ERROR' then 1 else 0 end as other_failed,
		             case when detail_info='请求auth，上线BAS错误' then 1 else 0 end as auth_bas_err,
		             case when detail_info='请求Challenge，上线BAS错误' then 1 else 0 end as cha_bas_err,
		             case when detail_info='请求Challenge被拒绝' then 1 else 0 end as cha_rejected,
		             case when detail_info='请求Challenge此链接已建立' then 1 else 0 end as cha_connected,
		             case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 1 else 0 end as auth_blocked,
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info='认证成功' then 1 else 0 end as success
	from temp_fixed_param_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;

	perform write_runlog(v_func_name,'insert rpt_scan_day2 start',0);
	--独立IP高频次攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, other_errors,
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,2 as scan_type, total as  scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success as other_errors,
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,success as success_total
        from (select ac_ip,user_type,login_type,count(*) as total,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected,sum(success) as success
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_matched,
		             case when detail_info='OBS访问失败' then 1 else 0 end as obs_failed,
		             case when detail_info='其他错误'  and err_type='OBS_ERROR' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误' and err_type='PORTAL_ERROR' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' and err_type='AC_ERROR'  then 1 else 0 end as other_failed,
		             case when detail_info='请求auth，上线BAS错误' then 1 else 0 end as auth_bas_err,
		             case when detail_info='请求Challenge，上线BAS错误' then 1 else 0 end as cha_bas_err,
		             case when detail_info='请求Challenge被拒绝' then 1 else 0 end as cha_rejected,
		             case when detail_info='请求Challenge此链接已建立' then 1 else 0 end as cha_connected,
		             case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 1 else 0 end as auth_blocked,
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info='认证成功' then 1 else 0 end as success
	from temp_ip_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;

	perform write_runlog(v_func_name,'insert rpt_scan_day3 start',0);
	--独立帐号高频次攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, other_errors,
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,3 as scan_type, total as  scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success as other_errors,
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total, success as success_total
        from (select ac_ip,user_type,login_type,count(*) as total,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected,sum(success) as success
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_matched,
		             case when detail_info='OBS访问失败' then 1 else 0 end as obs_failed,
		             case when detail_info='其他错误'  and err_type='OBS_ERROR' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误' and err_type='PORTAL_ERROR'  then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' and err_type='AC_ERROR'  then 1 else 0 end as other_failed,
		             case when detail_info='请求auth，上线BAS错误' then 1 else 0 end as auth_bas_err,
		             case when detail_info='请求Challenge，上线BAS错误' then 1 else 0 end as cha_bas_err,
		             case when detail_info='请求Challenge被拒绝' then 1 else 0 end as cha_rejected,
		             case when detail_info='请求Challenge此链接已建立' then 1 else 0 end as cha_connected,
		             case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 1 else 0 end as auth_blocked,
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info='认证成功' then 1 else 0 end as success
	from temp_account_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_apache_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_apache_log_day';
begin
	---------天调用。 从IP段得到省份信息。

	---------临时表出来， 计算IP属于那个省份。
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert temp_userip_prov start',0);
	
	truncate table temp_userip_prov;
	insert into temp_userip_prov(user_ip_txt, user_ip, prov_id)
	select user_ip_txt, user_ip, case when b.prov_id is null then -1 else b.prov_id end as prov_id 
	from(

		select user_ip as user_ip_txt, check_ip(user_ip) as user_ip 
		from( 
			select user_ip
			from temp_APACHE_LOG
			group by user_ip
		) t
	) a
	left join SYS_prov_ipseg_info b on  a.user_ip between b.start_ip and b.end_ip;

	perform write_runlog(v_func_name,'insert SRC_APACHE_LOG start',0);
	---------外部表入库， 入中间表
	insert into SRC_APACHE_LOG(ODATE, PROV_ID, USER_IP, PAGE_TYPE, STATUS_CODE, p_count)
	select ODATE, prov_id, b.USER_IP, PAGE_TYPE, STATUS_CODE, sum(p_count)
	from temp_APACHE_LOG a
	left join temp_userip_prov b on a.user_ip = b.user_ip_txt
	where b.prov_id != -1
	group by  ODATE, prov_id, b.USER_IP, PAGE_TYPE, STATUS_CODE;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_apache_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_apache_log_hour';
begin
	-------分时间段调用， APACHE日志入库。

	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert temp_APACHE_LOG start',0);

	truncate table temp_APACHE_LOG;
	------------外部表提前入临时表
	insert into temp_APACHE_LOG(ODATE, USER_IP, PAGE_TYPE, STATUS_CODE, p_count)
	select ODATE, user_ip
		, case when c.page_id is null then -1 else c.page_id end as page_type, status_code, count(1)
	from (
		select trim(arr_1[1]) as USER_IP, to_date(trim(arr_1[2]), 'dd/mon/yyyy:hh24:mi:ss +ms') as ODATE, 
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
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_apache_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_apache_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	---------------按日期、省份统计， 推送成功数、PV、UV、潜在用户数、访问介绍页面IP数
	perform write_runlog(v_func_name,'delete rpt_apache_day start',0);
	delete from rpt_apache_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	perform write_runlog(v_func_name,'insert rpt_apache_day start',0);
	insert into rpt_apache_day(odate, prov_id, SUCCESS_NUM, PV_NUM, UV_NUM, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select  a.odate, a.prov_id, COALESCE(SUCCESS_NUM, 0), COALESCE(PV_NUM, 0), COALESCE(UV_NUM, 0), COALESCE(POTENTIAL_USER_NUM, 0)
		, COALESCE(INTRO_PAGE_NUM,0)
	from (	
		-------------访问成功数、portal首页请求总量
		select odate, prov_id, sum(case when page_type = 1 and STATUS_CODE = 200 then p_count else 0 end ) as SUCCESS_NUM
			, sum(case when page_type = 1 then p_count else 0 end ) as PV_NUM
		from SRC_APACHE_LOG 
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) a 
	left join (
		-------------独立IP数
		select odate, prov_id, sum(case when STATUS_CODE = 200 and page_type = 1 then 1 else 0 end) as UV_NUM
		from (
			select odate, prov_id, user_ip, page_type, STATUS_CODE
			from SRC_APACHE_LOG
			where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
			group by odate, prov_id, user_ip, page_type, STATUS_CODE
		) tmp 
		group by odate, prov_id
	) b on a.odate = b.odate and a.prov_id = b.prov_id
	left join (
		-------------潜在用户数， 访问介绍页面数
		select odate, prov_id, sum(case when POTENTIAL_USER = 1 then 1 else 0 end ) as POTENTIAL_USER_NUM
			, sum(case when POTENTIAL_USER = 1 and INTRO_PAGE = 1 then 1 else 0 end) as INTRO_PAGE_NUM
		from SRC_APACHE_MONTH_LOG
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) c on a.odate = c.odate and a.prov_id = c.prov_id;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_src_apache_month_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_src_apache_month_log_day';
begin

	perform write_runlog(v_func_name,'function start',0);
	--------统计apcher 潜在用户（月累计， 每天聚合）
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if exists(select 1 from pg_partitions where lower(tablename)=lower('src_apache_month_log') and partitionname = v_partition_name) then
		execute ' alter table src_apache_month_log truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table src_apache_month_log add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert SRC_APACHE_MONTH_LOG start',0);
	---------所有的的USER_IP 需要进入 SRC_APACHE_MONTH_LOG表。
	insert into SRC_APACHE_MONTH_LOG(odate, prov_id, user_ip, POTENTIAL_USER, INTRO_PAGE, uv_flag)
	select a.odate, a.prov_id, a.user_ip, max(case when page_type = 1 and a.status_code = 200 and b.user_ip is null then 1 else 0 end) as POTENTIAL_USER
		, max(case when PAGE_TYPE=-1 or PAGE_TYPE=1 then 0 else 1 end) as INTRO_PAGE
		, max(case when page_type = 1 then 1 else 0 end) as uv_flag
	from (
		select a.odate, a.prov_id, a.user_ip, status_code
		from SRC_APACHE_LOG a
		where a.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by a.odate, a.prov_id, a.user_ip, status_code
	) a 
	left join temp_online_user_ip b on a.user_ip = b.user_ip and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	left join SRC_APACHE_LOG c on a.user_ip = c.user_ip and c.odate = to_date(vi_dealdate, 'yyyy-mm-dd') 
	group by a.odate, a.prov_id, a.user_ip;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_page_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_rpt_page_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------APCHE日志, 页面统计
	perform write_runlog(v_func_name,'delete rpt_page_day start',0);
	delete from rpt_page_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_page_day start',0);
	insert into rpt_page_day(odate, prov_id, PAGE_TYPE, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.odate, a.prov_id, a.PAGE_TYPE, sum(potential_user) as POTENTIAL_USER_NUM
		, INTRO_PAGE_NUM
	from (select odate, prov_id, page_type,sum(p_count) as INTRO_PAGE_NUM
		from SRC_APACHE_LOG 
		where page_type <> -1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id, page_type
	)a
	left join SRC_APACHE_MONTH_LOG b on a.prov_id = b.prov_id and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by a.odate, a.prov_id, a.page_type, INTRO_PAGE_NUM;

	perform write_runlog(v_func_name,'delete RPT_INTRO_PAGE_DAY start',0);
	-------------分省份访问介绍页面的IP
	delete from RPT_INTRO_PAGE_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	perform write_runlog(v_func_name,'insert RPT_INTRO_PAGE_DAY start',0);
	insert into RPT_INTRO_PAGE_DAY(odate, prov_id, USER_IP)
	select odate, prov_id, USER_IP
	from SRC_APACHE_MONTH_LOG
	where potential_user = 1 and intro_page = 1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by odate, prov_id, user_ip;

	-------------分页访问介绍页面的IP日累计表
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	
	if exists(select 1 from pg_partitions where lower(tablename)=lower('rpt_apache_log_add_day') and partitionname = v_partition_name) then
		execute ' alter table rpt_apache_log_add_day truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table rpt_apache_log_add_day add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert rpt_apache_log_add_day start',0);
	insert into rpt_apache_log_add_day(odate, prov_id, page_type, user_ip)
	select odate, prov_id, page_type, user_ip
	from SRC_APACHE_LOG
	where page_type <>-1 and page_type <>1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd');


	--判断是否存在抽取当天分区，没有则增加
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if not exists(select 1 from pg_partitions where lower(tablename)=lower('TEMP_APACHE_LOG_ADD_DAY') and partitionname = v_partition_name) then
		execute ' alter table TEMP_APACHE_LOG_ADD_DAY add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert TEMP_APACHE_LOG_ADD_DAY start',0);
	insert into TEMP_APACHE_LOG_ADD_DAY(odate, user_ip, prov_id, page_type, p_count)
	select odate, user_ip, prov_id, page_type, p_count
	from SRC_APACHE_LOG 
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_apache_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_apache_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------统计Portal访问相关信息, 月报表统计

	perform write_runlog(v_func_name,'delete rpt_apache_month start',0);
	delete from rpt_apache_month where odate = to_date(vi_dealdate, 'yyyy-mm'); 

	perform write_runlog(v_func_name,'insert rpt_apache_month start',0);
	insert into rpt_apache_month(odate, prov_id, SUCCESS_NUM, PV_NUM, UV_NUM, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.m_odate, a.prov_id, COALESCE(SUCCESS_NUM,0), COALESCE(PV_NUM,0), COALESCE(UV_NUM,0), COALESCE(POTENTIAL_USER_NUM,0), COALESCE(INTRO_PAGE_NUM, 0)
	from (
		select date_trunc('month', odate) :: date as m_odate, prov_id, sum(SUCCESS_NUM) as SUCCESS_NUM
			,  sum(PV_NUM) as PV_NUM
		from RPT_APACHE_DAY
		where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm')
		group by m_odate, prov_id
	) a
	left join (
		select m_odate, prov_id, sum(case when u_count = u_sum then 1 else 0 end ) as POTENTIAL_USER_NUM
			, sum(case when u_count = u_sum and intro_page_num = 1 then 1 else 0 end  ) as INTRO_PAGE_NUM
			, sum(UV_NUM) as UV_NUM
		from (
			select date_trunc('month', odate) :: date as m_odate, prov_id, user_ip
				, max(potential_user)  as POTENTIAL_USER_NUM
				, max(intro_page) as INTRO_PAGE_NUM
				, max(uv_flag)  as UV_NUM
				, count(1) as u_count, sum(potential_user) as u_sum
			from SRC_APACHE_MONTH_LOG
			where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm')
			group by m_odate, prov_id, user_ip
		) a
		group by m_odate, prov_id
	) b on a.m_odate = b.m_odate and a.prov_id = b.prov_id;	

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_wlan_auth_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_wlan_auth_day';
begin
	----------WLAN认证阶段分析
	perform write_runlog(v_func_name,'delete RPT_WLAN_AUTH_DAY start',0);
	delete from RPT_WLAN_AUTH_DAY where odate  = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert RPT_WLAN_AUTH_DAY start',0);
	insert into RPT_WLAN_AUTH_DAY(odate, prov_id, PORTAL_INDEX_REQUEST, AC_PUSH_PORTAL, ALL_LOGIN_REQUEST, NORMAL_LOGIN_REQUEST
		,CHA_REQUEST, AUTH_REQUEST, RADIUS_REQUEST, AUTH_SUCCESS)
	SELECT a.odate, a.prov_id, COALESCE(PORTAL_INDEX_REQUEST, 0), COALESCE(AC_PUSH_PORTAL, 0)
		, COALESCE(ALL_LOGIN_REQUEST, 0), COALESCE(NORMAL_LOGIN_REQUEST, 0)
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL, 0) as CHA_REQUEST
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL - ERR_NUM, 0) as AUTH_REQUEST
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL - ERR_NUM - auth_ERR_NUM, 0) as RADIUS_REQUEST
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL - ERR_NUM - auth_ERR_NUM - radius_auth_ERR_NUM, 0) as AUTH_SUCCESS
	FROM (
		-----
		select odate,  prov_ID, COALESCE(PV_NUM , 0)as PORTAL_INDEX_REQUEST
			, COALESCE(SUCCESS_NUM, 0) AC_PUSH_PORTAL
		from RPT_APACHE_DAY
		where odate  = to_date(vi_dealdate, 'yyyy-mm-dd')
	) a
	left join (
		select a.odate, a.prov_id, sum( COALESCE(a.other_errors, 0) + COALESCE(a.failed_total, 0) + COALESCE(a.success_total,0)
			+ COALESCE(b.other_errors, 0) + COALESCE(b.failed_total, 0) + COALESCE(b.success_total,0) ) as ALL_LOGIN_REQUEST
		from RPT_SCAN_DAY a
		left join RPT_NORMAL_REQUEST_DAY b on a.odate = b.odate and a.prov_id = b.prov_id
		where a.odate = to_date(vi_dealdate, 'yyyy-mm-dd') and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by a.odate, a.prov_id
	) b on a.prov_id = b.prov_id
	left join (
		select a.odate, a.prov_id, sum( COALESCE(a.other_errors, 0) + COALESCE(a.failed_total, 0) + COALESCE(a.success_total,0)) as NORMAL_LOGIN_REQUEST
		from RPT_NORMAL_REQUEST_DAY a
		group by a.odate, a.prov_id
	) c on a.prov_id = c.prov_id
	left join (
		select odate , prov_id, sum(COALESCE(WRONG_PWD, 0) + COALESCE(NO_SUBSCRIPTION, 0)
			+ COALESCE(WRONG_STATUS, 0) + COALESCE(AUTO_EXPIRED, 0) 
			+ COALESCE(PWD_EXPIRED, 0) + COALESCE(CARD_EXPIRED, 0)
			+ COALESCE(NO_WLAN_TIME, 0) + COALESCE(OBS_FAILED, 0) 
			+ COALESCE(OTHER_OBS_FAILED, 0) + COALESCE(OTHER_PORTAL_FAILED, 0) ) as FAILED_TOTAL 
		from RPT_NORMAL_REQUEST_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) d on a.prov_id = d.prov_id
	left join (
		select odate, prov_id, sum(COALESCE(ERR_NUM, 0)) as ERR_NUM 
		from RPT_CHA_NASIP_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) e on a.prov_id = e.prov_id
	left join (
		select odate, prov_id, COALESCE(CONNECTED_NUM, 0) + COALESCE(blocked_num, 0) +  COALESCE(bas_err_num, 0) as auth_ERR_NUM 
		from RPT_AUTH_REQUEST_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	) f on a.prov_id = f.prov_id
	left join (
		select odate, prov_id, sum(COALESCE(limit3, 0) + COALESCE(wrong_pwd, 0) + COALESCE(dns_not_found, 0) + COALESCE(eap_timeout, 0)) as radius_auth_ERR_NUM 
		from RPT_RADIUS_AUTH_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) g on a.prov_id = g.prov_id;

	perform write_runlog(v_func_name,'function end',0);
end

$BODY$
  LANGUAGE plpgsql VOLATILE;