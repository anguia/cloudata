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
        from (select ac_ip,user_type,login_type, sum(num) as total,
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
        from (select ac_ip,user_type,login_type, sum(num) as total,
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
        from (select ac_ip,user_type,login_type,sum(num) as total,
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