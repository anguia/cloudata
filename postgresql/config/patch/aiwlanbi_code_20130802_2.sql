CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm-dd
	
begin

	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	--清理当前统计日期下的数据
        delete from rpt_active_ua_type_day where odate = to_date(vi_dealdate,'yyyy-mm-dd');

        --统计终端类型活跃用户数
	insert into rpt_active_ua_type_day(odate,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm-dd'),
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,1,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm-dd')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm-dd')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm-dd')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm-dd')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent
	) n
	group by to_date(vi_dealDate,'yyyy-mm-dd'),ua_type ;

	--统计省份、终端类型活跃用户数
	insert into rpt_active_ua_type_day(odate,prov_id,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,2,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.node_id
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.node_id
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm-dd')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm-dd')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm-dd')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm-dd')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent,a.node_id
	) n
	group by to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,ua_type ;

	--统计省份、用户类型、终端类型维度下的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_ua_type_day(odate,prov_id,user_type,ua_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,
	       (case when n.customer_type = 2 then 4
		     when position('EDU.' in upper(n.user_name)) > 0 and n.customer_type = 0 then 2
		     when position('STARBUCKS' in upper(n.user_name)) > 0 and n.customer_type = 0 then 3
		     else 1 end) user_type,
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,99,COALESCE(sum(n.wlan_time),0),COALESCE(sum(n.input_octets+n.output_octets),0),count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.node_id,a.customer_type,a.wlan_time,a.input_octets,a.output_octets
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.node_id,t.customer_type,t.wlan_time,t.input_octets,t.output_octets
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm-dd')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm-dd')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm-dd')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm-dd')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		
	) n
	group by to_date(vi_dealDate,'yyyy-mm-dd'),n.node_id,user_type,ua_type ;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	
begin

	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm'); 

	--清理当前统计日期下的数据
	delete from rpt_active_ua_type_month where odate = to_date(vi_dealdate,'yyyy-mm');

	--统计终端类型活跃用户数
	insert into rpt_active_ua_type_month(odate,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm'),
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,1,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent
	) n
	group by to_date(vi_dealDate,'yyyy-mm'),ua_type ;

	--统计省份、终端类型活跃用户数
	insert into rpt_active_ua_type_month(odate,prov_id,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm'),n.node_id,
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,2,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.node_id
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.node_id
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent,a.node_id
	) n
	group by to_date(vi_dealDate,'yyyy-mm'),n.node_id,ua_type ;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_apache_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	---------------按日期、省份统计， 推送成功数、PV、UV、潜在用户数、访问介绍页面IP数
	delete from rpt_apache_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	insert into rpt_apache_day(odate, prov_id, SUCCESS_NUM, PV_NUM, UV_NUM, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select  a.odate, a.prov_id, COALESCE(SUCCESS_NUM, 0), COALESCE(PV_NUM, 0), COALESCE(UV_NUM, 0), COALESCE(POTENTIAL_USER_NUM, 0)
		, COALESCE(INTRO_PAGE_NUM,0)
	from (	
		-------------访问成功数、portal首页请求总量
		select odate, prov_id, sum(case when STATUS_CODE = 200 then p_count else 0 end ) as SUCCESS_NUM
			, sum(case when page_type = 1 then p_count else 0 end ) as PV_NUM
		from SRC_APACHE_LOG 
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) a 
	left join (
		-------------独立IP数
			select odate, prov_id, sum(case when page_type = 1 then 1 else 0 end) as UV_NUM
			from (
				select odate, prov_id, user_ip, page_type
				from SRC_APACHE_LOG
				where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
				group by odate, prov_id, user_ip, page_type
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
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_apache_month(vi_dealdate text)
  RETURNS void AS
$BODY$

begin

	--------统计Portal访问相关信息, 月报表统计
	delete from rpt_apache_month where odate = to_date(vi_dealdate, 'yyyy-mm'); 
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
			, sum(case when INTRO_PAGE = 1 then 1 else 0 end ) as INTRO_PAGE_NUM
			, sum(case when uv_flag = 1 then 1 else 0 end) as UV_NUM
		from (
			select date_trunc('month', odate) :: date as m_odate, prov_id, user_ip, potential_user, intro_page, uv_flag
				, count(1) as u_count, sum(potential_user) as u_sum
			from SRC_APACHE_MONTH_LOG
			where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm')
			group by m_odate, prov_id, user_ip, potential_user, intro_page, uv_flag
		) a
		group by m_odate, prov_id
	) b on a.m_odate = b.m_odate and a.prov_id = b.prov_id;	
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
	--where detail_info in ('请求AUTH此链接已建立数','请求AUTH有一个用户正在认证过程中，请稍后再试次数','请求AUTH，上线BAS错误数')
	where detail_info in ('认证请求被拒绝','请求AUTH，上线BAS错误数')
		and not exists (select 1 from src_radius_log s where t.date_time = s.date_time and  t.user_name = s.user_name and t.ac_ip= s.nas_ip)
	group by prov_id;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_monitor_log_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	---------------分布式固定参数攻击表，数据来源于MONITOR日志，表结构也与之相同,80%的数据占比
	truncate table TEMP_FIXED_PARAM_ATTACK;
	insert into TEMP_FIXED_PARAM_ATTACK(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type,  DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, a.stype, a.err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and exists(select 1 from (
		select user_ip,	ac_ip
		from src_MONITOR_LOG
		where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and user_ip is not null and ac_ip is not null
		group by user_ip, ac_ip 
		having count(1)>1000
	) b where a.user_ip=b.user_ip and a.ac_ip = b.ac_ip);

	truncate table temp_monitor_log_1;
	insert into TEMP_MONITOR_LOG_1(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and not exists(select * from (
		select user_ip,	ac_ip
		from TEMP_FIXED_PARAM_ATTACK
		where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
		group by user_ip, ac_ip)b where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.ac_ip=b.ac_ip and a.user_ip=b.user_ip);	

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
		
	truncate table TEMP_MONITOR_LOG_2;
	insert into TEMP_MONITOR_LOG_2(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_1 a
	where not exists(select * from (
		select user_ip
		from TEMP_IP_ATTACK
		group by user_ip)b where a.user_ip=b.user_ip) ;

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

	----------------正常用户上线请求表，数据来源于MONITOR日志，10%的数据占比
	truncate table TEMP_NORMAL_LOGIN_REQUEST;
	insert into TEMP_NORMAL_LOGIN_REQUEST(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  TEMP_MONITOR_LOG_2 a
	where  not exists(select 1 from (
		select user_name
		from TEMP_ACCOUNT_ATTACK
		group by user_name)T1 where t1.user_name = a.user_name);

	--钻取登录成功的数据保存到TEMP_LOGIN_REQUEST_SUCCESS临时表，用于和话单关联
	--execute 'alter table TEMP_LOGIN_REQUEST_SUCCESS truncate partition '||v_partition_name;
	delete from TEMP_LOGIN_REQUEST_SUCCESS where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd');
	insert into TEMP_LOGIN_REQUEST_SUCCESS(date_time,user_name,user_domain,user_agent)
	select date_time,case when position('@' in user_name)>1 then substring(user_name,1,position('@' in user_name)-1) else user_name end as user_name,
		case when position('@' in user_name)>1 then substring(user_name,position('@' in user_name)+1) else null end as user_domain
		,user_agent
	from src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
	and detail_info='认证成功';
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;