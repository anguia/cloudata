CREATE TABLE temp_monitor_fixparam_attack
(
  user_ip inet,
  ac_ip inet
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (user_ip);

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
	       (case when a.user_name ~ '[0-9]{11}' then 1
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
	      (case when a.user_name ~ '[0-9]{11}' then 1
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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
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
		             sum(case when detail_info='认证成功' then 1 else 0 end) as success
	from temp_fixed_param_attack group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
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
		             sum(case when detail_info='认证成功' then 1 else 0 end) as success
	from temp_ip_attack group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
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
        from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
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
		             sum(case when detail_info='认证成功' then 1 else 0 end) as success
	from temp_account_attack group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
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

	perform write_runlog(v_func_name,'insert temp_monitor_fixparam_attack start',0);
	truncate table temp_monitor_fixparam_attack;
	insert into temp_monitor_fixparam_attack(user_ip,ac_ip)
	select user_ip,	ac_ip
		from src_MONITOR_LOG
		where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and user_ip is not null and ac_ip is not null
		group by user_ip, ac_ip 
		having count(1)>1000;
		
	perform write_runlog(v_func_name,'insert TEMP_FIXED_PARAM_ATTACK start',0);
	---------------分布式固定参数攻击表，数据来源于MONITOR日志，表结构也与之相同,80%的数据占比
	truncate table TEMP_FIXED_PARAM_ATTACK;
	insert into TEMP_FIXED_PARAM_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type,  DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a,temp_monitor_fixparam_attack b
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.user_ip=b.user_ip and a.ac_ip = b.ac_ip;

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

