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
		select DATE_TIME, USER_NAME, user_ip,ac_ip, op_type, stype , err_type, DETAIL_INFO, USER_AGENT
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