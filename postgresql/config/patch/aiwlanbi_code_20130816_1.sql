CREATE OR REPLACE FUNCTION etl_rpt_cha_request_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_cha_request_day';
begin
	--按日期、省份、错误类型、acip统计错误数量到表rpt_cha_nasip_day
	--错误原因包括：
	--1.请求Challenge此链接已建立；2.请求Challenge被拒绝；3.请求Challenge有一个用户正在认证过程中，请稍后再试；
	--4.其他错误(portal根据Acname参数无法找到对应的ACIP) ；5.请求Challenge，上线BAS错误 ；6.接收AC/BAS响应包超时 ；7.AC名称不匹配 
	--8.用户上线且使用同一用户名和IP重复登录

	--'其他错误(portal根据Acname参数无法找到对应的ACIP)'  取得的条件为 (detail_info ='其他错误' and err_type='AC_ERROR')

	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_cha_nasip_day start',0);
	delete from rpt_cha_nasip_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_cha_nasip_day start',0);
	insert into rpt_cha_nasip_day(odate, prov_id, err_reason, nas_ip, err_num)
	select odate,ac_prov_id as prov_id,t.err_reason,t.nas_ip,sum(err_num) as err_num
	from (select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,ac_ip,case when detail_info='其他错误' 
		then '其他错误(portal根据Acname参数无法找到对应的ACIP)' else detail_info end as err_reason,ac_ip as nas_ip,count(1) as err_num
	from TEMP_NORMAL_LOGIN_REQUEST
	where detail_info in ('请求Challenge此链接已建立','请求Challenge被拒绝','请求Challenge有一个用户正在认证过程中，请稍后再试','请求Challenge，上线BAS错误','接收AC/BAS响应包超时','AC名称不匹配',
		'用户上线且使用同一用户名和IP重复登录') or (detail_info ='其他错误' and err_type='AC_ERROR')
	group by detail_info,ac_ip) t,sys_prov_acip_info c  
	where t.nas_ip = c.ac_ip group by t.odate,c.ac_prov_id,err_reason,t.nas_ip;
	

	--按日期省份统计challenge错误的数量到表rpt_cha_request_day
	perform write_runlog(v_func_name,'delete rpt_cha_request_day start',0);
	delete from rpt_cha_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_cha_request_day start',0);
	insert into rpt_cha_request_day(odate, prov_id, connected, rejected, blocked, no_acip_found, bas_err, timeout, acname_not_matched, dup_login)
	select odate,prov_id,sum(case when err_reason='请求Challenge此链接已建立' then err_num else 0 end) as connected,sum(case when err_reason='请求Challenge被拒绝' then err_num else 0 end) as rejected,
		sum(case when err_reason='请求Challenge有一个用户正在认证过程中，请稍后再试' then err_num else 0 end) as blocked,sum(case when err_reason='其他错误(portal根据Acname参数无法找到对应的ACIP)' then err_num else 0 end) as no_acip_found,
		sum(case when err_reason='请求Challenge，上线BAS错误' then err_num else 0 end) as bas_err,sum(case when err_reason='接收AC/BAS响应包超时' then err_num else 0 end) as timeout,
		sum(case when err_reason='AC名称不匹配' then err_num else 0 end) as acname_not_matched,sum(case when err_reason='用户上线且使用同一用户名和IP重复登录' then err_num else 0 end) as dup_login
	from rpt_cha_nasip_day
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd') 
	group by odate,prov_id;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_no_subscription_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_m_tel text[];
	v_nm_tel text[];
begin
	--------用户没有订购业务分析
	--移动号码
	select string_to_array(string_agg(msisdn_header,','), ',') 
	into v_m_tel
	from SYS_TELE_PROVIDER a
	where provider_id = 1;

	--其他厂商号码
	select string_to_array(string_agg(msisdn_header,','), ',') 
	into v_nm_tel
	from SYS_TELE_PROVIDER a
	where provider_id <> 1;

	delete from RPT_NO_SUBSCRIPTION_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--根据特征取各种类型的号码统计
	insert into RPT_NO_SUBSCRIPTION_DAY(odate, prov_id, mobile_num, other_num, err_num)
	select odate,b.ac_prov_id as prov_id
		, sum(case when mn_flag = 1 then p_count else 0 end) as  mobile_num
		, sum(case when mn_flag = 2 then p_count else 0 end) as  other_num
		, sum(case when mn_flag = 3 then p_count else 0 end) as  err_num
	from (
		select date_trunc('day', date_time) as odate,ac_ip, fn_subscription(user_name, v_m_tel, v_nm_tel) as mn_flag, count(1) as p_count
		from TEMP_NORMAL_LOGIN_REQUEST
		group by odate, ac_ip, mn_flag
	)a  
	left join sys_prov_acip_info b on a.ac_ip = b.ac_ip
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd') and b.ac_prov_id is not null
	group by odate, prov_id;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;