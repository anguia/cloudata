ALTER TABLE widget_param
   ALTER COLUMN value TYPE character varying(255);
COMMENT ON COLUMN widget_param.value IS '参数值';

ALTER TABLE temp_account_attack
   ALTER COLUMN date_time TYPE date;
ALTER TABLE temp_account_attack
  ADD COLUMN num numeric DEFAULT 0;
COMMENT ON COLUMN temp_account_attack.num IS '分组统计数量';


ALTER TABLE temp_fixed_param_attack
   ALTER COLUMN date_time TYPE date;
ALTER TABLE temp_fixed_param_attack
  ADD COLUMN num numeric DEFAULT 0;
COMMENT ON COLUMN temp_fixed_param_attack.num IS '分组统计数量';


ALTER TABLE temp_ip_attack
   ALTER COLUMN date_time TYPE date;
ALTER TABLE temp_ip_attack
  ADD COLUMN num numeric DEFAULT 0;
COMMENT ON COLUMN temp_ip_attack.num IS '分组统计数量';


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
	insert into TEMP_FIXED_PARAM_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type,  DETAIL_INFO, USER_AGENT,NUM)
	select to_date(vi_dealDate, 'yyyy-mm-dd'), a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT,count(*)
	from  src_MONITOR_LOG a,temp_monitor_fixparam_attack b
	where op_type='webauth_logon' and date_trunc('day', a.DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.user_ip=b.user_ip and a.ac_ip = b.ac_ip
	group by a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT;

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
	insert into TEMP_IP_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT,NUM)
	select to_date(vi_dealDate, 'yyyy-mm-dd') as DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, err_type, a.DETAIL_INFO, a.USER_AGENT,count(*)
	from  TEMP_MONITOR_LOG_1 a 
	where exists(select * from (
		select user_ip
		from TEMP_MONITOR_LOG_1
		group by user_ip
		having count(1) > 100
	) b where  a.user_ip=b.user_ip )
	group by a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, err_type, a.DETAIL_INFO, a.USER_AGENT;

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
	insert into TEMP_ACCOUNT_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT,NUM)
	select to_date(vi_dealDate, 'yyyy-mm-dd') as DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT,count(*)
	from  TEMP_MONITOR_LOG_2 a
	where  exists(select 1 from (
		select user_name
		from TEMP_MONITOR_LOG_2
		where detail_info='用户密码错误'
		group by user_name
		having count(1)>50)T1 where t1.user_name = a.user_name)
	group by a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT;

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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, count(*) as total,
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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, count(*) as total,
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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, count(*) as total,
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


CREATE TABLE temp_active_user_day
(
  node_id integer,
  bms_node_id integer,
  user_name character(64),
  nas_ip inet,
  user_type integer,
  login_type integer,
  wlan_time numeric,
  in_out_octets numeric
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED RANDOMLY;

CREATE TABLE temp_active_user_month
(
  node_id integer,
  bms_node_id integer,
  user_name character(64),
  nas_ip inet,
  user_type integer,
  login_type integer,
  wlan_time numeric,
  in_out_octets numeric
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED RANDOMLY;



CREATE OR REPLACE FUNCTION etl_src_subscription()
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_src_subscription';
begin
	perform write_runlog(v_func_name,'function start',0);

	perform write_runlog(v_func_name,'insert SRC_SUBSCRIPTION start',0);
	
	--从外部表导入订购数据到订购表源表
	insert into src_subscription (bms_user_name,bms_user_password,bms_product_id,bms_subscription_status,
	                              bms_subscription_status_time,bms_node_id,bms_customer_type,bms_subscription_begin_time,
	                              bms_subscription_id,bms_create_time)
	select t.bms_user_name,
	       t.bms_user_password,
	       t.bms_product_id::numeric,
	       t.bms_subscription_status::smallint,
	       to_timestamp(t.bms_subscription_status_time,'yyyy-mm-dd hh24:mi:ss'),
	       t.bms_node_id::integer,
	       t.bms_customer_type::integer,
	       to_timestamp(t.bms_subscription_begin_time,'yyyy-mm-dd hh24:mi:ss'),
	       t.bms_subscription_id::numeric,
	       to_timestamp(t.bms_create_time,'yyyy-mm-dd hh24:mi:ss')
	from ext_subscription t;

	perform write_runlog(v_func_name,'function end',0);
end

$BODY$
  LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION etl_src_wlan_package()
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_src_wlan_package';
begin
	perform write_runlog(v_func_name,'function start',0);

	perform write_runlog(v_func_name,'insert src_wlan_package start',0);
	
	--从外部表导入wlan资源叠加包数据到源表，只导入'00011','00012','00013'三种需要统计的类型，减少数据量
	insert into src_wlan_package (bms_user_name,package_code,package_name,abs_effect_time,abs_expire_time,bms_prefix_type,abs_res_open,time_stamp)
	select t.bms_user_name,
	       t.package_code,
	       t.package_name,
	       to_timestamp(t.abs_effect_time,'yyyy-mm-dd hh24:mi:ss'),
	       to_timestamp(t.abs_expire_time,'yyyy-mm-dd hh24:mi:ss'),
	       t.bms_prefix_type::integer,
	       t.abs_res_open::numeric,
	       to_timestamp(t.time_stamp,'yyyy-mm-dd hh24:mi:ss')
	from ext_wlan_package t
	where t.package_code in ('00011','00012','00013');

	perform write_runlog(v_func_name,'function end',0);
end

$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_src_wlan_user_cookie()
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_src_wlan_user_cookie';
begin
	perform write_runlog(v_func_name,'function start',0);

	perform write_runlog(v_func_name,'insert src_wlan_user_cookie start',0);
	
	--从外部表导入数据到wlan用户cookie记录源表
	insert into src_wlan_user_cookie (bms_user_name,abs_effect_time,abs_expire_time,abs_effect_days,ua_type,time_stamp)
	select t.bms_user_name,
	       to_timestamp(t.abs_effect_time,'yyyy-mm-dd hh24:mi:ss'),
	       to_timestamp(t.abs_expire_time,'yyyy-mm-dd hh24:mi:ss'),
	       t.abs_effect_days::integer,
	       t.ua_type::integer,
	       to_timestamp(t.time_stamp,'yyyy-mm-dd hh24:mi:ss')
	from ext_wlan_user_cookie t;

	perform write_runlog(v_func_name,'function end',0);
end

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
	       (case when t.user_name ~ '[0-9]{11}' then 1
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
	       (case when t.user_name ~ '[0-9]{11}' then 1
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