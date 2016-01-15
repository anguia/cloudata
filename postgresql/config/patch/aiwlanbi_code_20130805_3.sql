CREATE OR REPLACE FUNCTION etl_rpt_cha_request_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	--按日期、省份、错误类型、acip统计错误数量到表rpt_cha_nasip_day
	--错误原因包括：
	--1.请求Challenge此链接已建立；2.请求Challenge被拒绝；3.请求Challenge有一个用户正在认证过程中，请稍后再试；
	--4.其他错误(portal根据Acname参数无法找到对应的ACIP) ；5.请求Challenge，上线BAS错误 ；6.接收AC/BAS响应包超时 ；7.AC名称不匹配 
	--8.用户上线且使用同一用户名和IP重复登录

	--'其他错误(portal根据Acname参数无法找到对应的ACIP)'  取得的条件为 (detail_info ='其他错误' and err_type='AC_ERROR')
	
	delete from rpt_cha_nasip_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	insert into rpt_cha_nasip_day(odate, prov_id, err_reason, nas_ip, err_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,case when detail_info='其他错误' 
		then '其他错误(portal根据Acname参数无法找到对应的ACIP)' else detail_info end as err_reason,ac_ip as nas_ip,count(1) as err_num
	from TEMP_NORMAL_LOGIN_REQUEST
	where detail_info in ('请求Challenge此链接已建立','请求Challenge被拒绝','请求Challenge有一个用户正在认证过程中，请稍后再试','请求Challenge，上线BAS错误','接收AC/BAS响应包超时','AC名称不匹配',
		'用户上线且使用同一用户名和IP重复登录') or (detail_info ='其他错误' and err_type='AC_ERROR')
	group by prov_id,detail_info,ac_ip;
	

	--按日期省份统计challenge错误的数量到表rpt_cha_request_day
	delete from rpt_cha_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	insert into rpt_cha_request_day(odate, prov_id, connected, rejected, blocked, no_acip_found, bas_err, timeout, acname_not_matched, dup_login)
	select odate,prov_id,sum(case when err_reason='请求Challenge此链接已建立' then err_num else 0 end) as connected,sum(case when err_reason='请求Challenge被拒绝' then err_num else 0 end) as rejected,
		sum(case when err_reason='请求Challenge有一个用户正在认证过程中，请稍后再试' then err_num else 0 end) as blocked,sum(case when err_reason='其他错误(portal根据Acname参数无法找到对应的ACIP)' then err_num else 0 end) as no_acip_found,
		sum(case when err_reason='请求Challenge，上线BAS错误' then err_num else 0 end) as bas_err,sum(case when err_reason='接收AC/BAS响应包超时' then err_num else 0 end) as timeout,
		sum(case when err_reason='AC名称不匹配' then err_num else 0 end) as acname_not_matched,sum(case when err_reason='用户上线且使用同一用户名和IP重复登录' then err_num else 0 end) as dup_login
	from rpt_cha_nasip_day
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd') 
	group by odate,prov_id,err_reason;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_normal_request_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	delete from rpt_normal_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--正常用户上线请求统计
	insert into rpt_normal_request_day(odate, prov_id, user_type, login_type, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total,success_total
        from (select ac_ip,user_type,login_type,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected,sum(success) as success_total
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 4
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 1 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 1 end ) login_type, 
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
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝(星巴克）' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info in ('AC名称不匹配','OBS访问失败','用户没有订购业务','用户密码错误',
				'用户状态错误','自动认证已过期(cookie)','动态密码有效期过期',
				'用户上线且使用同一用户名和IP重复登录','用户先上线,然后用另一名字在同一客户机器再认证','认证请求被拒绝',
				'用户没有可用时长','用户卡无效','读取OBS响应包超时','接收AC/BAS响应包超时','其他错误','请求auth，上线BAS错误',
				'请求Challenge，上线BAS错误','请求Challenge被拒绝','请求Challenge此链接已建立',
				'请求Challenge有一个用户正在认证过程中，请稍后再试','认证请求被拒绝(星巴克）') then 0 else 1 end as success
	from temp_normal_login_request) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_pwd_err_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	delete from rpt_pwd_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--根据省份，统计部分成功数和完全失败的数
	--此处“其他错误”已经包括“其他错误(OBS)”+“其他错误（PORTAL）”+“其他错误（AC）”
	insert into rpt_pwd_err_day(odate, prov_id, part_failed_num, all_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,
		sum(case when all_num<>fail_num then 1 else 0 end) as part_failed_num,
		sum(case when all_num=fail_num then 1 else 0 end) as all_failed_num
	 from (select prov_id,count(*) as all_num,
		sum(case when detail_info in ('AC名称不匹配','OBS访问失败','用户没有订购业务 ','用户密码错误','用户状态错误',
			'自动认证已过期(cookie)','动态密码有效期过期','用户上线且使用同一用户名和IP重复登录','用户先上线,然后用另一名字在同一客户机器再认证',
			'认证请求被拒绝','用户没有可用时长','用户卡无效','读取OBS响应包超时','接收AC/BAS响应包超时','其他错误','请求auth，上线BAS错误',
			'请求Challenge，上线BAS错误','请求Challenge被拒绝','请求Challenge此链接已建立','请求Challenge有一个用户正在认证过程中，请稍后再试',
			'认证请求被拒绝(星巴克）') then 1 else 0 end) as fail_num
	from temp_normal_login_request group by prov_id,user_ip) t group by prov_id;

end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_scan_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	delete from rpt_scan_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--分布式固定参数攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,1 as scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total, 
            (scan_num - (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)) as success_total
        from (select ac_ip,user_type,login_type,count(*) as scan_num,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 4
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 1 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 1 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_mached,
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
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝(星巴克）' then 1 else 0 end as starbuck_auth_rejected
	from temp_fixed_param_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;

	--独立IP高频次攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,2 as scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total, 
            (scan_num - (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)) as success_total
        from (select ac_ip,user_type,login_type,count(*) as scan_num,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 4
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 1 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 1 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_mached,
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
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝(星巴克）' then 1 else 0 end as starbuck_auth_rejected
	from temp_ip_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;

	--独立帐号高频次攻击相关数据统计
	insert into rpt_scan_day(odate, prov_id, user_type, login_type, scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            failed_total, success_total)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,s.ac_prov_id as prov_id, user_type, login_type,3 as scan_type, scan_num, acname_not_matched, 
            obs_failed, other_obs_failed, no_subscription, wrong_pwd, wrong_status, 
            other_portal_failed, auto_expired, pwd_expired, dup_ip_user, 
            dup_auth, auth_rejected, no_wlan_time, card_expired, obs_resp_expired, 
            ac_bas_resp_expired, other_failed, auth_bas_err, cha_bas_err, 
            cha_rejected, cha_connected, auth_blocked, starbuck_auth_rejected, 
            (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected) as failed_total, 
            (scan_num - (acname_not_matched+obs_failed+other_obs_failed+no_subscription+wrong_pwd+wrong_status+other_portal_failed
            +auto_expired+pwd_expired+dup_ip_user+dup_auth+auth_rejected+no_wlan_time+card_expired+obs_resp_expired+ac_bas_resp_expired
            +other_failed+auth_bas_err+cha_bas_err+cha_rejected+cha_connected+auth_blocked+starbuck_auth_rejected)) as success_total
        from (select ac_ip,user_type,login_type,count(*) as scan_num,sum(acname_not_matched) as acname_not_matched, 
            sum(obs_failed) as obs_failed, sum(other_obs_failed) as other_obs_failed, sum(no_subscription) as no_subscription, 
            sum(wrong_pwd) as wrong_pwd, sum(wrong_status) as wrong_status,sum(other_portal_failed) as other_portal_failed, 
            sum(auto_expired) as auto_expired, sum(pwd_expired) as pwd_expired, sum(dup_ip_user) as dup_ip_user, 
            sum(dup_auth) as dup_auth, sum(auth_rejected) as auth_rejected, sum(no_wlan_time) as no_wlan_time, sum(card_expired) as card_expired, sum(obs_resp_expired) as obs_resp_expired, 
            sum(ac_bas_resp_expired) as ac_bas_resp_expired, sum(other_failed) as other_failed, sum(auth_bas_err) as auth_bas_err, sum(cha_bas_err) as cha_bas_err, 
            sum(cha_rejected) as cha_rejected, sum(cha_connected) as cha_connected, sum(auth_blocked) as auth_blocked, sum(starbuck_auth_rejected) as  starbuck_auth_rejected
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 4
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 1 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 1 end ) login_type, 
		             case when detail_info='AC名称不匹配' then 1 else 0 end as acname_not_mached,
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
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝(星巴克）' then 1 else 0 end as starbuck_auth_rejected
	from temp_account_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

DROP EXTERNAL TABLE ext_cboss_log;

CREATE EXTERNAL TABLE ext_cboss_log (
    part1 text,
    part2 text,
    part3 text,
    part4 text,
    part5 text,
    part6 text,
    part7 text,
    part8 text,
    part9 text,
    part10 text,
    part11 text,
    part12 text,
    part13 text,
    part14 text,
    part15 text,
    part16 text,
    part17 text,
    part18 text,
    part19 text,
    part20 text,
    part21 text
) LOCATION (
    'gpfdist://10.3.3.138:8004/monitor*'
) FORMAT 'text' (delimiter E'{' null E'\\N' escape E'\\' fill missing fields)
ENCODING 'GBK'
LOG ERRORS INTO public.err_ext_cboss_log SEGMENT REJECT LIMIT 200000000 ROWS;
