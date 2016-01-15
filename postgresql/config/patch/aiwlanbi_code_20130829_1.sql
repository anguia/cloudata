ALTER TABLE temp_fixed_param_attack
  ADD COLUMN user_type integer DEFAULT 0;
ALTER TABLE temp_fixed_param_attack
  ADD COLUMN login_type integer DEFAULT 0;

ALTER TABLE temp_ip_attack
  ADD COLUMN user_type integer DEFAULT 0;
ALTER TABLE temp_ip_attack
  ADD COLUMN login_type integer DEFAULT 0;


ALTER TABLE temp_account_attack
  ADD COLUMN user_type integer DEFAULT 0;
ALTER TABLE temp_account_attack
  ADD COLUMN login_type integer DEFAULT 0;  





ALTER TABLE temp_account_attack
  DROP COLUMN user_name;

ALTER TABLE temp_ip_attack
  DROP COLUMN user_name;

ALTER TABLE TEMP_FIXED_PARAM_ATTACK
  DROP COLUMN user_name;


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
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
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
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
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
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
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
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,1 as scan_type, sum(total) as scan_num, sum(acname_not_matched), 
            sum(obs_failed), sum(other_obs_failed), sum(no_subscription), sum(wrong_pwd), sum(wrong_status), 
            sum(other_portal_failed), sum(auto_expired), sum(pwd_expired), sum(dup_ip_user), 
            sum(dup_auth), sum(auth_rejected), sum(no_wlan_time), sum(card_expired), sum(obs_resp_expired), 
            sum(ac_bas_resp_expired), sum(other_failed), sum(auth_bas_err), sum(cha_bas_err), 
            sum(cha_rejected), sum(cha_connected), sum(auth_blocked), sum(starbuck_auth_rejected), 
            sum(total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success) as other_errors,
            sum(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,sum(success) as success_total
        from (select ac_ip,user_type,login_type, count(*) as total,
		             sum(case when detail_info='AC名称不匹配' then num else 0 end) as acname_not_matched,
		             sum(case when detail_info='OBS访问失败' then num else 0 end) as obs_failed,
		             sum(case when detail_info='其他错误' and err_type='OBS_ERROR' then num else 0 end) as other_obs_failed,
		             sum(case when detail_info='用户没有订购业务' then num else 0 end) as no_subscription,
		             sum(case when detail_info='用户密码错误' then num else 0 end) as wrong_pwd,
		             sum(case when detail_info='用户状态错误' then num else 0 end) as wrong_status,
		             sum(case when detail_info='其他错误' and err_type='PORTAL_ERROR' then num else 0 end) as other_portal_failed,
		             sum(case when detail_info='自动认证已过期(cookie)' then num else 0 end) as auto_expired,
		             sum(case when detail_info='动态密码有效期过期' then num else 0 end) as pwd_expired,
		             sum(case when detail_info='用户上线且使用同一用户名和IP重复登录' then num else 0 end) as dup_ip_user,
		             sum(case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then num else 0 end) as dup_auth,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT115' then num else 0 end) as auth_rejected,
		             sum(case when detail_info='用户没有可用时长' then num else 0 end) as no_wlan_time,
		             sum(case when detail_info='用户卡无效' then num else 0 end) as card_expired,
		             sum(case when detail_info='读取OBS响应包超时' then num else 0 end) as obs_resp_expired,
		             sum(case when detail_info='接收AC/BAS响应包超时' then num else 0 end) as ac_bas_resp_expired,
		             sum(case when detail_info='其他错误' and err_type='AC_ERROR' then num else 0 end) as other_failed,
		             sum(case when detail_info='请求auth，上线BAS错误' then num else 0 end) as auth_bas_err,
		             sum(case when detail_info='请求Challenge，上线BAS错误' then num else 0 end) as cha_bas_err,
		             sum(case when detail_info='请求Challenge被拒绝' then num else 0 end) as cha_rejected,
		             sum(case when detail_info='请求Challenge此链接已建立' then num else 0 end) as cha_connected,
		             sum(case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then num else 0 end) as auth_blocked,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT999' then num else 0 end) as starbuck_auth_rejected,
		             sum(case when detail_info='认证成功' then num else 0 end) as success
	from temp_fixed_param_attack group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip group by s.ac_prov_id,t2.user_type,t2.login_type;

	perform write_runlog(v_func_name,'insert rpt_scan_day2 start',0);
	--独立IP高频次攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, other_errors,
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,2 as scan_type, sum(total) as  scan_num, sum(acname_not_matched), 
            sum(obs_failed), sum(other_obs_failed), sum(no_subscription), sum(wrong_pwd), sum(wrong_status), 
            sum(other_portal_failed), sum(auto_expired), sum(pwd_expired), sum(dup_ip_user), 
            sum(dup_auth), sum(auth_rejected), sum(no_wlan_time), sum(card_expired), sum(obs_resp_expired), 
            sum(ac_bas_resp_expired), sum(other_failed), sum(auth_bas_err), sum(cha_bas_err), 
            sum(cha_rejected), sum(cha_connected), sum(auth_blocked), sum(starbuck_auth_rejected), 
            sum(total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success) as other_errors,
            sum(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,sum(success) as success_total
        from (select ac_ip,user_type,login_type, count(*) as total,
		             sum(case when detail_info='AC名称不匹配' then num else 0 end) as acname_not_matched,
		             sum(case when detail_info='OBS访问失败' then num else 0 end) as obs_failed,
		             sum(case when detail_info='其他错误' and err_type='OBS_ERROR' then num else 0 end) as other_obs_failed,
		             sum(case when detail_info='用户没有订购业务' then num else 0 end) as no_subscription,
		             sum(case when detail_info='用户密码错误' then num else 0 end) as wrong_pwd,
		             sum(case when detail_info='用户状态错误' then num else 0 end) as wrong_status,
		             sum(case when detail_info='其他错误' and err_type='PORTAL_ERROR' then num else 0 end) as other_portal_failed,
		             sum(case when detail_info='自动认证已过期(cookie)' then num else 0 end) as auto_expired,
		             sum(case when detail_info='动态密码有效期过期' then num else 0 end) as pwd_expired,
		             sum(case when detail_info='用户上线且使用同一用户名和IP重复登录' then num else 0 end) as dup_ip_user,
		             sum(case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then num else 0 end) as dup_auth,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT115' then num else 0 end) as auth_rejected,
		             sum(case when detail_info='用户没有可用时长' then num else 0 end) as no_wlan_time,
		             sum(case when detail_info='用户卡无效' then num else 0 end) as card_expired,
		             sum(case when detail_info='读取OBS响应包超时' then num else 0 end) as obs_resp_expired,
		             sum(case when detail_info='接收AC/BAS响应包超时' then num else 0 end) as ac_bas_resp_expired,
		             sum(case when detail_info='其他错误' and err_type='AC_ERROR' then num else 0 end) as other_failed,
		             sum(case when detail_info='请求auth，上线BAS错误' then num else 0 end) as auth_bas_err,
		             sum(case when detail_info='请求Challenge，上线BAS错误' then num else 0 end) as cha_bas_err,
		             sum(case when detail_info='请求Challenge被拒绝' then num else 0 end) as cha_rejected,
		             sum(case when detail_info='请求Challenge此链接已建立' then num else 0 end) as cha_connected,
		             sum(case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then num else 0 end) as auth_blocked,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT999' then num else 0 end) as starbuck_auth_rejected,
		             sum(case when detail_info='认证成功' then num else 0 end) as success
	from temp_ip_attack group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip group by s.ac_prov_id,t2.user_type,t2.login_type;

	perform write_runlog(v_func_name,'insert rpt_scan_day3 start',0);
	--独立帐号高频次攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, other_errors,
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,3 as scan_type, sum(total) as  scan_num, sum(acname_not_matched), 
            sum(obs_failed), sum(other_obs_failed), sum(no_subscription), sum(wrong_pwd), sum(wrong_status), 
            sum(other_portal_failed), sum(auto_expired), sum(pwd_expired), sum(dup_ip_user), 
            sum(dup_auth), sum(auth_rejected), sum(no_wlan_time), sum(card_expired), sum(obs_resp_expired), 
            sum(ac_bas_resp_expired), sum(other_failed), sum(auth_bas_err), sum(cha_bas_err), 
            sum(cha_rejected), sum(cha_connected), sum(auth_blocked), sum(starbuck_auth_rejected), 
            sum(total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success) as other_errors,
            sum(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total, sum(success) as success_total
        from (select ac_ip,user_type,login_type,count(*) as total,
		             sum(case when detail_info='AC名称不匹配' then num else 0 end) as acname_not_matched,
		             sum(case when detail_info='OBS访问失败' then num else 0 end) as obs_failed,
		             sum(case when detail_info='其他错误' and err_type='OBS_ERROR' then num else 0 end) as other_obs_failed,
		             sum(case when detail_info='用户没有订购业务' then num else 0 end) as no_subscription,
		             sum(case when detail_info='用户密码错误' then num else 0 end) as wrong_pwd,
		             sum(case when detail_info='用户状态错误' then num else 0 end) as wrong_status,
		             sum(case when detail_info='其他错误' and err_type='PORTAL_ERROR' then num else 0 end) as other_portal_failed,
		             sum(case when detail_info='自动认证已过期(cookie)' then num else 0 end) as auto_expired,
		             sum(case when detail_info='动态密码有效期过期' then num else 0 end) as pwd_expired,
		             sum(case when detail_info='用户上线且使用同一用户名和IP重复登录' then num else 0 end) as dup_ip_user,
		             sum(case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then num else 0 end) as dup_auth,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT115' then num else 0 end) as auth_rejected,
		             sum(case when detail_info='用户没有可用时长' then num else 0 end) as no_wlan_time,
		             sum(case when detail_info='用户卡无效' then num else 0 end) as card_expired,
		             sum(case when detail_info='读取OBS响应包超时' then num else 0 end) as obs_resp_expired,
		             sum(case when detail_info='接收AC/BAS响应包超时' then num else 0 end) as ac_bas_resp_expired,
		             sum(case when detail_info='其他错误' and err_type='AC_ERROR' then num else 0 end) as other_failed,
		             sum(case when detail_info='请求auth，上线BAS错误' then num else 0 end) as auth_bas_err,
		             sum(case when detail_info='请求Challenge，上线BAS错误' then num else 0 end) as cha_bas_err,
		             sum(case when detail_info='请求Challenge被拒绝' then num else 0 end) as cha_rejected,
		             sum(case when detail_info='请求Challenge此链接已建立' then num else 0 end) as cha_connected,
		             sum(case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then num else 0 end) as auth_blocked,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT999' then num else 0 end) as starbuck_auth_rejected,
		             sum(case when detail_info='认证成功' then num else 0 end) as success
	from temp_account_attack group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip group by s.ac_prov_id,t2.user_type,t2.login_type;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION etl_monitor_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_monitor_log_hour';
begin
	perform write_runlog(v_func_name,'function start',0);
	--判断是否存在对应的分区，没有则增加
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if not exists(select 1 from pg_partitions where lower(tablename)=lower('src_MONITOR_LOG') and partitionname = v_partition_name) then
		execute ' alter table src_MONITOR_LOG add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	--else
		--execute ' alter table src_MONITOR_LOG truncate partition ' || v_partition_name || ';';
	end if;

	perform write_runlog(v_func_name,'insert temp_src_MONITOR_LOG start',0);
	--从外部表抽取数据到src原始数据表(临时表)，注意acip为空的不需要抽取
	truncate table temp_src_MONITOR_LOG;
	insert into temp_src_MONITOR_LOG(DATE_TIME, USER_NAME, USER_IP, AC_IP, op_type, stype, err_type, DETAIL_INFO, USER_AGENT)
	select * from (
		select DATE_TIME, USER_NAME, user_ip,ac_ip, substr(op_type,1,100) as op_type, stype , err_type, substr(op_type,1,100) as DETAIL_INFO, USER_AGENT
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

	perform write_runlog(v_func_name,'insert temp_monitor_userip start',0);
	truncate table temp_monitor_userip;
	insert into temp_monitor_userip(user_ip,inet_ip)
	select t.user_ip,check_ip(t.user_ip) as inet_ip 
	from temp_src_MONITOR_LOG t group by user_ip;

	perform write_runlog(v_func_name,'insert temp_monitor_acip start',0);
	truncate table temp_monitor_acip;
	insert into temp_monitor_acip(ac_ip,inet_ip)
	select t.ac_ip,check_ip(t.ac_ip) as inet_ip 
	from temp_src_MONITOR_LOG t group by ac_ip;

	perform write_runlog(v_func_name,'insert src_MONITOR_LOG start',0);
	insert into src_MONITOR_LOG(DATE_TIME, USER_NAME, USER_IP, AC_IP, op_type, stype, err_type, DETAIL_INFO, USER_AGENT)
	select s.DATE_TIME, s.USER_NAME, u.inet_ip as USER_IP, a.inet_ip as AC_IP, s.op_type, s.stype, s.err_type, s.DETAIL_INFO, s.USER_AGENT
	from temp_src_MONITOR_LOG s 
		left join temp_monitor_userip u on s.user_ip=u.user_ip 
		left join temp_monitor_acip a on s.ac_ip=a.ac_ip;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_radius_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_radius_log_hour';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------------- RADIUS 执行表
	
	truncate table temp_src_radius_log;
	
	perform write_runlog(v_func_name,'insert temp_src_radius_log start',0);

	--判断是否存在对应的分区，没有则增加
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if not exists(select 1 from pg_partitions where lower(tablename)=lower('temp_src_radius_log') and partitionname = v_partition_name) then
		execute ' alter table temp_src_radius_log add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	else
		execute ' alter table temp_src_radius_log truncate partition ' || v_partition_name || ';';
	end if;

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
	)b where length(result_type) <= 50 and length(AUTHEN_TYPE) <= 50;


	perform write_runlog(v_func_name,'insert temp_src_radius_log_nas_ip start',0);
	truncate table temp_src_radius_log_nas_ip;
	------------IP转换，处理非法的NAS_IP
	insert into temp_src_radius_log_nas_ip(nas_ip_txt, nas_ip) 
	select nas_ip, check_ip(nas_ip)
	from (
		select nas_ip 
		from temp_src_radius_log
		group by nas_ip
	) a ;

	perform write_runlog(v_func_name,'insert src_radius_log start',0);

	--判断是否存在抽取当天分区，没有则增加
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if not exists(select 1 from pg_partitions where lower(tablename)=lower('SRC_RADIUS_LOG') and partitionname = v_partition_name) then
		execute ' alter table SRC_RADIUS_LOG add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;


	----------临时数据进入src_radius_log表
	insert into src_radius_log(date_time, user_name, nas_ip, MAC, result, result_type, AUTHEN_TYPE)
	select date_time, user_name, b.nas_ip, MAC, result, result_type, AUTHEN_TYPE
	from temp_src_radius_log a 
	left join temp_src_radius_log_nas_ip b on a.nas_ip = b.nas_ip_txt;
	
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_ip(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_ip';
begin

	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert sys_prov_ipseg_info start',0);

	--插入ip段局数据
	insert into sys_prov_ipseg_info(idx, prov_id, subnet_mask, start_ip, end_ip, create_time, update_time)
	SELECT idx, prov_id, subnet_mask, start_ip, end_ip, create_time, update_time
        FROM temp_sys_prov_ipseg_info
        where date_trunc('day',create_time) <= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'delete temp_sys_prov_ipseg_info start',0);
	
	delete from temp_sys_prov_ipseg_info
	where date_trunc('day',create_time) <= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert sys_prov_acip_info start',0);

	--插入acip局数据
	insert into sys_prov_acip_info(idx, ac_prov_id, ac_city_name, ac_name, ac_ip, create_time, update_time)
        SELECT idx, ac_prov_id, ac_city_name, ac_name, ac_ip, create_time, update_time
        FROM temp_sys_prov_acip_info
        where date_trunc('day',create_time) <= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'delete temp_sys_prov_acip_info start',0);
	
        delete from temp_sys_prov_acip_info
        where date_trunc('day',create_time) <= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_cboss_monitor_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_partition_name text; --表分区名称
	v_func_name text:='etl_rpt_cboss_monitor_day';
begin

	perform write_runlog(v_func_name,'function start',0);
		
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');

	--检查表分区是否存在，不存在则新建，存在则删除
	if exists(select 1 from pg_partitions where lower(tablename)=lower('rpt_cboss_monitor_day') and partitionname = v_partition_name) then
		perform write_runlog(v_func_name,'truncate rpt_active_user_month partition start',0);
		execute ' alter table rpt_cboss_monitor_day truncate partition ' || v_partition_name || ';';
	else 
		perform write_runlog(v_func_name,'add rpt_active_user_month partition start',0);
		execute ' alter table rpt_cboss_monitor_day add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert rpt_cboss_monitor_day start',0);
	
	--从外部表导入cboss数据，只导入BIP2B147的
	insert into rpt_cboss_monitor_day(odate, bip_code, trans_id, biz_type, opr_code, user_name, sp_biz_code, 
            user_status, process_time, opr_time, efft_time, rsp_desc, orig_domain, home_prov)
	select b.odate,b.bip_code,b.trans_id,b.biz_type,b.opr_code,b.user_name,b.sp_biz_code,b.user_status,b.process_time,b.opr_time,b.efft_time,b.rsp_desc,b.orig_domain,b.home_prov
	from (
		select to_timestamp(a.arr[1]||' '||a.arr[2],'yyyy/mm/dd hh24:mi:ss') odate,a.bip_code,a.trans_id,a.biz_type,a.opr_code,a.user_name,a.sp_biz_code,a.user_status,
		       a.process_time,a.opr_time,a.efft_time,a.rsp_desc,a.orig_domain,a.home_prov
		from (
			select string_to_array(part1,' ') arr,
			       trim(part2) bip_code,
			       trim(part4) trans_id,
			       trim(part5) biz_type,
			       trim(part6) opr_code,
			       trim(part7) user_name,
			       trim(part8) sp_biz_code,
			       trim(part9) user_status,
			       to_timestamp(trim(part11),'yyyymmddhh24miss') process_time,
			       to_timestamp(trim(part12),'yyyymmddhh24miss') opr_time,
			       to_timestamp(trim(part13),'yyyymmddhh24miss') efft_time,
			       trim(part15) rsp_desc,
			       trim(part17) orig_domain,
			       COALESCE(trim(part20),'-1')::integer home_prov
			from ext_cboss_log 
			where trim(part2) = 'BIP2B147'
		) a
	)b
        where date_trunc('day',odate)=to_date(vi_dealdate,'yyyy-mm-dd');

        perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_active_ua_type_day';
	
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_active_ua_type_day start',0);
	
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	
	--清理当前统计日期下的数据
        delete from rpt_active_ua_type_day where odate = to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_active_ua_type_day(stat_type=1) start',0);
	
        --统计终端类型活跃用户数
	insert into rpt_active_ua_type_day(odate,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm-dd'),
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,1,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.user_domain
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.user_domain
			from src_usage t
			where date_trunc('day', t.start_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where date_trunc('day', t.date_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent,a.user_domain
	) n
	group by to_date(vi_dealDate,'yyyy-mm-dd'),ua_type ;

	perform write_runlog(v_func_name,'insert rpt_active_ua_type_day(stat_type=2) start',0);
	
	--统计省份、终端类型活跃用户数
	insert into rpt_active_ua_type_day(odate,prov_id,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,2,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.node_id,a.user_domain
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.node_id,t.user_domain
			from src_usage t
			where date_trunc('day', t.start_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where date_trunc('day', t.date_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent,a.node_id,a.user_domain
	) n
	group by to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,ua_type ;

	perform write_runlog(v_func_name,'insert rpt_active_ua_type_day(stat_type=99) start',0);

	--统计省份、用户类型、终端类型维度下的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_ua_type_day(odate,prov_id,user_type,ua_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,
	       (case when n.user_name ~ '^[0-9]{11}$' then 1
		     when position('EDU.' in upper(n.user_name)) > 0 then 2
		     when position('STARBUCKS' in upper(n.user_name)) > 0 then 3
		     else 4 end) user_type,
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,99,COALESCE(sum(n.wlan_time),0),COALESCE(sum(n.input_octets+n.output_octets),0),count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.node_id,a.customer_type,a.wlan_time,a.input_octets,a.output_octets,a.user_domain
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.node_id,t.customer_type,t.wlan_time,t.input_octets,t.output_octets,t.user_domain
			from src_usage t
			where date_trunc('day', t.start_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where date_trunc('day', t.date_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		
	) n
	group by to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,user_type,ua_type ;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_user_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_active_user_day';
begin
	perform write_runlog(v_func_name,'function start',0);
		
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert temp_active_user_day start',0);
	
	--保存部分话单数据到临时表，减少查询压力
	truncate table temp_active_user_day;
	insert into temp_active_user_day(node_id, bms_node_id, user_name, nas_ip, user_type, login_type, 
					 wlan_time, in_out_octets)
        select t.node_id,t.bms_node_id,t.user_name,t.nas_ip,
	       (case when t.user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,
	       t.wlan_time,t.input_octets + t.output_octets
        from src_usage t
        where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd');

        perform write_runlog(v_func_name,'delete rpt_active_user_day start',0);
	--清理当前统计日期下的数据
	delete from rpt_active_user_day where odate= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=1) start',0);
	
	--统计集团活跃用户数
	insert into rpt_active_user_day(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from temp_active_user_day t
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_day(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from temp_active_user_day t
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=3) start',0);
	
	--统计归属地活跃用户数
	insert into rpt_active_user_day(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from temp_active_user_day t
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=4) start',0);
	
	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=5) start',0);
	
	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_day(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,t.user_type	       		
		from temp_active_user_day t
		group by t.user_name,t.user_type
	) a
	group by a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=6) start',0);
	
	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.user_type	       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.user_type
	) a
	group by a.node_id,a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=7) start',0);
	
	--统计登录方式维度的活跃用户数
	insert into rpt_active_user_day(odate,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.login_type,7,count(a.user_name)
	from(
		select t.user_name,t.login_type       		
		from temp_active_user_day t
		group by t.user_name,t.login_type
	) a
	group by a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=8) start',0);
	
	--统计使用地、登录方式维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.login_type,8,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.login_type       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.login_type
	) a
	group by a.node_id,a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=9) start',0);
	
	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=99) start',0);
	
	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type,99,sum(t.wlan_time),
	       sum(t.in_out_octets) in_out_octets,count(t.user_name)
	from temp_active_user_day t
	group by to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_active_user_month';
begin
	perform write_runlog(v_func_name,'function start',0);
		
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	perform write_runlog(v_func_name,'insert temp_active_user_month start',0);
	
	--保存部分话单数据到临时表，减少查询压力
	truncate table temp_active_user_month;
	insert into temp_active_user_month(node_id, bms_node_id, user_name, nas_ip, user_type, login_type, 
					 wlan_time, in_out_octets)
        select t.node_id,t.bms_node_id,t.user_name,t.nas_ip,
	       (case when t.user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,
	       t.wlan_time,t.input_octets + t.output_octets
        from src_usage t
        where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm');

        perform write_runlog(v_func_name,'delete rpt_active_user_month start',0);
	--清理当前统计日期下的数据
	delete from rpt_active_user_month where to_date(odate,'yyyy-mm')= to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=1) start',0);
	
	--统计集团活跃用户数
	insert into rpt_active_user_month(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from temp_active_user_month t
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_month(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from temp_active_user_month t
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=3) start',0);
	
	--统计归属地活跃用户数
	insert into rpt_active_user_month(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from temp_active_user_month t
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=4) start',0);
	
	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=5) start',0);
	
	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,t.user_type	       		
		from temp_active_user_month t
		group by t.user_name,t.user_type
	) a
	group by a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=6) start',0);
	
	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.user_type	       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.user_type
	) a
	group by a.node_id,a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=7) start',0);
	
	--统计登录方式维度的活跃用户数
	insert into rpt_active_user_month(odate,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.login_type,7,count(a.user_name)
	from(
		select t.user_name,t.login_type       		
		from temp_active_user_month t
		group by t.user_name,t.login_type
	) a
	group by a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=8) start',0);
	
	--统计使用地、登录方式维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.login_type,8,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.login_type       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.login_type
	) a
	group by a.node_id,a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=9) start',0);
	
	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=99) start',0);
	
	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type,99,sum(t.wlan_time) wlan_time,
	       sum(t.in_out_octets) in_out_octets,count(t.user_name) use_num
	from temp_active_user_month t
	group by to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type;

	perform write_runlog(v_func_name,'function end',0);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_hotspot_usage(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_hotspot_usage';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_hotspot_usage start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm')  ;

	--清理当前统计日期下的数据
	delete from rpt_hotspot_usage where to_date(odate,'yyyy-mm') = to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_hotspot_usage start',0);
	
	--统计各省公共热点、acip数量，校园热点、acip数量，热点使用人数0-10人、10-100人、100-500人、500-1000人、1000以上数量
	insert into rpt_hotspot_usage (odate,prov_id,pub_ac_num,pub_hotspot_num,edu_ac_num,edu_hotspot_num,
	                               hotspot_l10_num,hotspot_l100_num,hotspot_l500_num,hotspot_l1000_num,hotspot_g1000_num)
	select to_date(vi_dealdate,'yyyy-mm'),
	       n.prov_id,
	       COALESCE(n.pub_acip,0),
	       COALESCE(n.pub_identifier,0),
	       COALESCE(n.edu_acip,0),
	       COALESCE(n.edu_identifier,0),
	       COALESCE(n.ten_limit,0),
	       COALESCE(n.hundred_limit,0),
	       COALESCE(n.five_hundred_limit,0),
	       COALESCE(n.thousand_limit,0),
	       COALESCE(n.thousand_over,0)
	from (
		select prov.prov_id,c.pub_identifier,c.edu_identifier,d.pub_acip,d.edu_acip,
		       e.ten_limit,e.hundred_limit,e.five_hundred_limit,e.thousand_limit,e.thousand_over
		from sys_prov_info prov
	left join (

	--统计公共热点和校园热点数量
	select b.node_id,
	       sum(case when b.user_type=1 then b.cnt else 0 end ) pub_identifier,
	       sum(case when b.user_type=2 then b.cnt else 0 end ) edu_identifier
	from (
		select a.node_id,a.user_type,count(a.nas_identifier) cnt	       
		from (
			select t.node_id,
			       (case when position('EDU.' in upper(t.user_name)) > 0 then 2
				     when position('STARBUCKS' in upper(t.user_name)) > 0  then 3
				     when t.user_name ~ '^[0-9]{11}$' then 1 
				     else 4 end) user_type,
			       t.nas_identifier
			from src_usage t
			where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')			
			group by t.node_id,
				 user_type,
				 t.nas_identifier
		) a
		group by a.node_id,a.user_type
	)b 
	group by b.node_id 
	) c on c.node_id = prov.prov_id
	left join(

	--统计公共acip和校园acip数量
	select b.node_id,
	       sum(case when b.user_type=1 then b.cnt else 0 end ) pub_acip,
	       sum(case when b.user_type=2 then b.cnt else 0 end ) edu_acip
	from (
		select a.node_id,a.user_type,count(a.nas_ip) cnt	       
		from (
			select t.node_id,
			       (case when position('EDU.' in upper(t.user_name)) > 0 then 2
				     when position('STARBUCKS' in upper(t.user_name)) > 0  then 3
				     when t.user_name ~ '^[0-9]{11}$' then 1 
				     else 4 end) user_type,
			       t.nas_ip
			from src_usage t
			where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
			group by t.node_id,
				 user_type,
				 t.nas_ip
		) a
		group by a.node_id,a.user_type
	) b
	group by b.node_id
	) d on d.node_id = c.node_id
	left join (

	--统计使用人数0-10人、10-100人、100-500人、500-1000人、1000以上的热点数量
	select b.node_id,
	       sum(case when b.cnt <= 10 then 1 else 0 end) ten_limit,
	       sum(case when b.cnt >10 and b.cnt <=100 then 1 else 0 end) hundred_limit,
	       sum(case when b.cnt >100 and b.cnt <=500 then 1 else 0 end) five_hundred_limit,
	       sum(case when b.cnt >500 and b.cnt <=1000 then 1 else 0 end) thousand_limit,
	       sum(case when b.cnt >1000 then 1 else 0 end)thousand_over
	from (
		select a.node_id,a.nas_identifier,count(a.user_name) cnt
		from (
			select t.node_id,
			       t.user_name,
			       t.nas_identifier
			from src_usage t
			where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
			group by t.node_id,
				 t.user_name,
				 t.nas_identifier
		) a
		group by a.node_id,a.nas_identifier
	) b
	group by b.node_id
	) e on e.node_id = d.node_id ) n;
	
	perform write_runlog(v_func_name,'function end',0);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION etl_rpt_new_active_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	v_last_month text; --上一个月日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_new_active_user_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_new_active_user_month start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');
	v_last_month = to_char(to_date(vi_dealDate, 'yyyy-mm') - interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_new_active_user_month where to_date(odate,'yyyy-mm')=to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_new_active_user_month start',0);
	
	--统计新增活跃用户数
	insert into rpt_new_active_user_month (odate,prov_id,user_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,
	       (case when a.user_name ~ '^[0-9]{11}$' then 1
	             when position('EDU.' in upper(a.user_name)) > 0 then 2
	             when position('STARBUCKS' in upper(a.user_name)) > 0 then 3
	             else 4 end),count(a.user_name)
	from (
		select t.node_id,
		       t.user_name		              
		from src_usage t
		where date_trunc('month', t.time_stamp) = to_date(vi_dealdate,'yyyy-mm')		
		group by t.node_id,t.user_name
	) a
	where not exists(
		select 1 from (
			select t.node_id,
			       t.user_name	       
			from src_usage t
			where date_trunc('month', t.time_stamp) = to_date(v_last_month,'yyyy-mm')			
			group by t.node_id,t.user_name
		) b 
		where b.node_id = a.node_id and b.user_name = a.user_name
	)
	group by to_date(vi_dealdate,'yyyy-mm'),a.node_id,
	      (case when a.user_name ~ '^[0-9]{11}$' then 1
	             when position('EDU.' in upper(a.user_name)) > 0 then 2
	             when position('STARBUCKS' in upper(a.user_name)) > 0 then 3
	             else 4 end);
	             
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
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, t2.user_type, t2.login_type, sum(acname_not_matched), 
            sum(obs_failed), sum(other_obs_failed), sum(no_subscription), sum(wrong_pwd), sum(wrong_status), 
            sum(other_portal_failed), sum(auto_expired), sum(pwd_expired), sum(dup_ip_user), 
            sum(dup_auth), sum(auth_rejected), sum(no_wlan_time), sum(card_expired), sum(obs_resp_expired), 
            sum(ac_bas_resp_expired), sum(other_failed), sum(auth_bas_err), sum(cha_bas_err), 
            sum(cha_rejected), sum(cha_connected), sum(auth_blocked), sum(starbuck_auth_rejected), 
            sum(total-(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)-success_total) as other_errors,
            sum(acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,sum(success_total)
        from (select ac_ip,(case when user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, count(*) as total,
		             sum(case when detail_info='AC名称不匹配' then 1 else 0 end) as acname_not_matched,
		             sum(case when detail_info='OBS访问失败' then 1 else 0 end) as obs_failed,
		             sum(case when detail_info='其他错误' and err_type='OBS_ERROR' then 1 else 0 end) as other_obs_failed,
		             sum(case when detail_info='用户没有订购业务' then 1 else 0 end) as no_subscription,
		             sum(case when detail_info='用户密码错误' then 1 else 0 end) as wrong_pwd,
		             sum(case when detail_info='用户状态错误' then 1 else 0 end) as wrong_status,
		             sum(case when detail_info='其他错误' and err_type='PORTAL_ERROR' then 1 else 0 end) as other_portal_failed,
		             sum(case when detail_info='自动认证已过期(cookie)' then 1 else 0 end) as auto_expired,
		             sum(case when detail_info='动态密码有效期过期' then 1 else 0 end) as pwd_expired,
		             sum(case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end) as dup_ip_user,
		             sum(case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end) as dup_auth,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end) as auth_rejected,
		             sum(case when detail_info='用户没有可用时长' then 1 else 0 end) as no_wlan_time,
		             sum(case when detail_info='用户卡无效' then 1 else 0 end) as card_expired,
		             sum(case when detail_info='读取OBS响应包超时' then 1 else 0 end) as obs_resp_expired,
		             sum(case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end) as ac_bas_resp_expired,
		             sum(case when detail_info='其他错误' and err_type='AC_ERROR' then 1 else 0 end) as other_failed,
		             sum(case when detail_info='请求auth，上线BAS错误' then 1 else 0 end) as auth_bas_err,
		             sum(case when detail_info='请求Challenge，上线BAS错误' then 1 else 0 end) as cha_bas_err,
		             sum(case when detail_info='请求Challenge被拒绝' then 1 else 0 end) as cha_rejected,
		             sum(case when detail_info='请求Challenge此链接已建立' then 1 else 0 end) as cha_connected,
		             sum(case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 1 else 0 end) as auth_blocked,
		             sum(case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end) as starbuck_auth_rejected,
		             sum(case when detail_info='认证成功' then 1 else 0 end) as success_total
	from temp_normal_login_request group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip group by s.ac_prov_id,t2.user_type,t2.login_type;
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION fn_subscription(vi_user_name text, vi_m_tel text[], vi_nm_tel text[])
  RETURNS integer AS
$BODY$
declare 
	v_tel_tmp text;
	v_i int4;
begin
	----------判断订购用户， 返回值 1：移动号码， 2：非移动号码， 3： 非号码
	----------select fn_subscription('jt.18211111111@web.pc', array['182', '183'], array['186', '111']);
	--以“jt.”开头的取接下来的11位为手机号码，否则直接取11位
	if position('jt.' in vi_user_name) = 1 then
		v_tel_tmp = substring(vi_user_name, 4, 11);
	else 
		v_tel_tmp = substring(vi_user_name, 1, 11);
	end if ;

	--如果不足11位则为非号码
	if length(v_tel_tmp) <> 11 then 
		return 3;
	end if ;

	--判断是否为11位数字
	if v_tel_tmp ~ '^[0-9]{11}$' then
		v_i = 0;
		---------判断移动号码
		for v_i in 1..array_upper(vi_m_tel,1) loop
			if position(vi_m_tel[v_i] in v_tel_tmp) = 1 then 
				return 1;
			end if;
		end loop;
		---------判断否移动号码
		v_i = 0;
		for v_i in 1..array_upper(vi_nm_tel,1) loop
			if position(vi_nm_tel[v_i] in v_tel_tmp) = 1 then 
				return 2;
			end if;
		end loop;
	end if; 
	return 3;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;