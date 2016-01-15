DROP TABLE temp_login_request_success;

CREATE TABLE temp_login_request_success
(
  date_time timestamp without time zone,
  user_name character varying(64),
  user_domain character varying(64),
  user_agent text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, COMPRESSTYPE=zlib, 
  OIDS=FALSE
)
DISTRIBUTED BY (date_time, user_name);


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
		             case when detail_info='其他错误(OBS)' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误（PORTAL）' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' then 1 else 0 end as other_failed,
		             case when detail_info='请求auth，上线BAS错误' then 1 else 0 end as auth_bas_err,
		             case when detail_info='请求Challenge，上线BAS错误' then 1 else 0 end as cha_bas_err,
		             case when detail_info='请求Challenge被拒绝' then 1 else 0 end as cha_rejected,
		             case when detail_info='请求Challenge此链接已建立' then 1 else 0 end as cha_connected,
		             case when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 1 else 0 end as auth_blocked,
		             case when detail_info='认证请求被拒绝(星巴克）' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info in ('AC名称不匹配','OBS访问失败','其他错误(OBS)','用户没有订购业务','用户密码错误',
				'用户状态错误','其他错误（PORTAL）','自动认证已过期(cookie)','动态密码有效期过期',
				'用户上线且使用同一用户名和IP重复登录','用户先上线,然后用另一名字在同一客户机器再认证','认证请求被拒绝',
				'用户没有可用时长','用户卡无效','读取OBS响应包超时','接收AC/BAS响应包超时','其他错误','请求auth，上线BAS错误',
				'请求Challenge，上线BAS错误','请求Challenge被拒绝','请求Challenge此链接已建立',
				'请求Challenge有一个用户正在认证过程中，请稍后再试','认证请求被拒绝(星巴克）') then 0 else 1 end as success
	from temp_normal_login_request) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
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
		             case when detail_info='其他错误(OBS)' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误（PORTAL）' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' then 1 else 0 end as other_failed,
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
		             case when detail_info='其他错误(OBS)' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误（PORTAL）' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' then 1 else 0 end as other_failed,
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
		             case when detail_info='其他错误(OBS)' then 1 else 0 end as other_obs_failed,
		             case when detail_info='用户没有订购业务' then 1 else 0 end as no_subscription,
		             case when detail_info='用户密码错误' then 1 else 0 end as wrong_pwd,
		             case when detail_info='用户状态错误' then 1 else 0 end as wrong_status,
		             case when detail_info='其他错误（PORTAL）' then 1 else 0 end as other_portal_failed,
		             case when detail_info='自动认证已过期(cookie)' then 1 else 0 end as auto_expired,
		             case when detail_info='动态密码有效期过期' then 1 else 0 end as pwd_expired,
		             case when detail_info='用户上线且使用同一用户名和IP重复登录' then 1 else 0 end as dup_ip_user,
		             case when detail_info='用户先上线,然后用另一名字在同一客户机器再认证' then 1 else 0 end as dup_auth,
		             case when detail_info='认证请求被拒绝' then 1 else 0 end as auth_rejected,
		             case when detail_info='用户没有可用时长' then 1 else 0 end as no_wlan_time,
		             case when detail_info='用户卡无效' then 1 else 0 end as card_expired,
		             case when detail_info='读取OBS响应包超时' then 1 else 0 end as obs_resp_expired,
		             case when detail_info='接收AC/BAS响应包超时' then 1 else 0 end as ac_bas_resp_expired,
		             case when detail_info='其他错误' then 1 else 0 end as other_failed,
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

CREATE OR REPLACE FUNCTION etl_rpt_auth_request_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	--按日期、省份、错误类型、acip统计错误数量到表rpt_cha_nasip_day
		
	delete from rpt_auth_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	insert into rpt_auth_request_day(odate, prov_id, connected_num, blocked_num, bas_err_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,
		sum(case when detail_info='认证请求被拒绝' then 1 else 0 end) as connected_num,-1 as blocked_num,
		--sum(case when detail_info='请求AUTH此链接已建立数' then 1 else 0 end) as connected_num,
		--sum(case when detail_info='请求AUTH有一个用户正在认证过程中，请稍后再试次数' then 1 else 0 end) as blocked_num,
		sum(case when detail_info='请求AUTH，上线BAS错误数' then 1 else 0 end) as bas_err_num
	from TEMP_NORMAL_LOGIN_REQUEST t
	where detail_info in ('请求AUTH此链接已建立数','请求AUTH有一个用户正在认证过程中，请稍后再试次数','请求AUTH，上线BAS错误数')
		and not exists (select 1 from src_radius_log s where t.date_time = s.date_time and  t.user_name = s.user_name and t.ac_ip= s.nas_ip)
	group by prov_id;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_temp_online_user_ip(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
begin
	--------统计MONITOR日志中在线用户数
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if exists(select 1 from pg_partitions where lower(tablename)=lower('TMP_ONLINE_USER_IP') and partitionname = v_partition_name) then
		execute ' alter table TMP_ONLINE_USER_IP truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table TMP_ONLINE_USER_IP add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;
	
	insert into TMP_ONLINE_USER_IP(odate, prov_id, user_ip)
	select odate, case when b.prov_id is null then -1 else prov_id end, user_ip
	from (
		select DATE_TIME:: date as odate, user_ip
		from SRC_MONITOR_LOG
		where op_type='webauth_logon' and user_ip is not null and DATE_TIME :: date = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, user_ip
	) a
	left join SYS_prov_ipseg_info b on a.user_ip between b.start_ip and b.end_ip;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_src_usage(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_partition_name text; --表分区名称
begin
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');

	--检查表分区是否存在，不存在则新建，存在则删除
	if exists(select 1 from pg_partitions where lower(tablename)=lower('SRC_USAGE') and partitionname = v_partition_name) then
		execute ' alter table SRC_USAGE truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table SRC_USAGE add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	--从外部表导入话单数据到话单源表
	insert into SRC_USAGE (start_time,stop_time,node_id,bms_node_id,customer_type,user_name,nas_ip,nas_identifier,
	                       wlan_time,input_octets,output_octets,user_domain,authen_type,time_stamp,mac_addr)
	select to_timestamp(trim(t.start_time),'yyyy-mm-dd hh24:mi:ss'),
	       to_timestamp(trim(t.stop_time),'yyyy-mm-dd hh24:mi:ss'),
	       trim(t.node_id)::integer,
	       trim(t.bms_node_id)::integer,
	       trim(t.customer_type)::integer,
	       trim(t.user_name),
	       trim(t.nas_ip)::inet,
	       trim(t.nas_identifier),
	       COALESCE(trim(t.session_time)::numeric,0),
	       COALESCE(trim(t.input_octets)::numeric,0),
	       COALESCE(trim(t.output_octets)::numeric,0),
	       trim(t.user_domain),
	       trim(t.authen_type)::integer,
	       to_timestamp(trim(t.time_stamp),'yyyy-mm-dd hh24:mi:ss'),
	       trim(t.caller_id)
	from ext_usage t;
end

$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_wlan_auth_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	----------WLAN认证阶段分析
	delete from RPT_WLAN_AUTH_DAY where odate  = to_date(vi_dealdate, 'yyyy-mm-dd');
	 
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
		select odate, prov_id, sum(COALESCE(ALL_LOGIN_REQUEST, 0)) as ALL_LOGIN_REQUEST
		from (
			select DATE_TIME :: date as odate, AC_IP, count(1) as ALL_LOGIN_REQUEST 
			from SRC_MONITOR_LOG
			where DATE_TIME :: date  = to_date(vi_dealdate, 'yyyy-mm-dd')
			group by odate, AC_IP
		) t1
		left join SYS_prov_ipseg_info t2 on t1.ac_ip between t2.start_ip and t2.end_ip
		group by odate, prov_id
	) b on a.prov_id = b.prov_id
	left join (
		select odate, prov_id, sum(COALESCE(NORMAL_LOGIN_REQUEST, 0)) as NORMAL_LOGIN_REQUEST
		from (
			select DATE_TIME :: date as odate, AC_IP, count(1) as NORMAL_LOGIN_REQUEST 
			from TEMP_NORMAL_LOGIN_REQUEST
			where DATE_TIME :: date  = to_date(vi_dealdate, 'yyyy-mm-dd')
			group by odate, AC_IP
		) t1
		left join SYS_prov_ipseg_info t2 on t1.ac_ip between t2.start_ip and t2.end_ip
		group by odate, prov_id
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
end

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

CREATE OR REPLACE FUNCTION etl_src_apache_month_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
begin
	--------统计apcher 潜在用户（月累计， 每天聚合）
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if exists(select 1 from pg_partitions where lower(tablename)=lower('src_apache_month_log') and partitionname = v_partition_name) then
		execute ' alter table src_apache_month_log truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table src_apache_month_log add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	---------出现INDEX.PHP的USER_IP 才需要进入 SRC_APACHE_MONTH_LOG表。
	insert into SRC_APACHE_MONTH_LOG(odate, prov_id, user_ip, POTENTIAL_USER, INTRO_PAGE, uv_flag)
	select a.odate, a.prov_id, a.user_ip, max(case when b.user_ip is null then 1 else 0 end) as POTENTIAL_USER
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
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;