CREATE OR REPLACE FUNCTION etl_rpt_page_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_partition_name text;
begin
	--------APCHE日志, 页面统计
	delete from rpt_page_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	insert into rpt_page_day(odate, prov_id, PAGE_TYPE, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.odate, a.prov_id, a.PAGE_TYPE, sum(potential_user) as POTENTIAL_USER_NUM
		, sum(case when potential_user = 1 then intro_page else 0 end) as INTRO_PAGE_NUM
	from (select odate, prov_id, page_type, user_ip 
		from SRC_APACHE_LOG 
		where page_type <> -1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id, page_type, user_ip
	)a
	left join SRC_APACHE_MONTH_LOG b on a.user_ip = b.user_ip and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by a.odate, a.prov_id, a.page_type;

	-------------分省份访问介绍页面的IP
	delete from RPT_INTRO_PAGE_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	insert into RPT_INTRO_PAGE_DAY(odate, prov_id, USER_IP)
	select odate, prov_id, USER_IP
	from SRC_APACHE_MONTH_LOG
	where potential_user = 1 and intro_page = 1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by odate, prov_id, user_ip  ;

	-------------分页访问介绍页面的IP日累计表
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	
	if exists(select 1 from pg_partitions where lower(tablename)=lower('rpt_apache_log_add_day') and partitionname = v_partition_name) then
		execute ' alter table rpt_apache_log_add_day truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table rpt_apache_log_add_day add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;
	
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
	insert into TEMP_APACHE_LOG_ADD_DAY(odate, user_ip, prov_id, page_type, p_count)
	select odate, user_ip, prov_id, page_type, p_count
	from SRC_APACHE_LOG 
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_page_month(vi_dealdate text)
  RETURNS void AS
$BODY$

begin

	--------统计Portal访问相关信息, 月报表统计
	
	truncate table temp_RPT_PAGE_MONTH;
	insert into temp_RPT_PAGE_MONTH(odate, prov_id, user_ip, PAGE_TYPE,p_count)
	select a.m_odate, a.prov_id, user_ip, PAGE_TYPE, p_count
	from (
		select date_trunc('month', odate) :: date as m_odate, prov_id, PAGE_TYPE, user_ip, sum(p_count) as p_count
		from TEMP_APACHE_LOG_ADD_DAY 
		where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm') 
		group by m_odate, prov_id, PAGE_TYPE, user_ip
	) a;

	delete from RPT_PAGE_MONTH where odate = to_date(vi_dealdate, 'yyyy-mm'); 

	insert into RPT_PAGE_MONTH(odate, prov_id, PAGE_TYPE, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.odate, a.prov_id, a.page_type, COALESCE(POTENTIAL_USER_NUM, 0), COALESCE(INTRO_PAGE_NUM, 0)
	from (  select odate, prov_id, page_type, count(1) as POTENTIAL_USER_NUM
		from temp_RPT_PAGE_MONTH tmp1
		where not exists(select 1 from TEMP_ONLINE_USER_IP t1 where t1.user_ip = tmp1.user_ip)
		group by odate, prov_id, page_type
	) a
	left join (
		select odate, prov_id, page_type, sum(p_count) as INTRO_PAGE_NUM
		from temp_RPT_PAGE_MONTH
		where page_type <> -1 and page_type <> 1
		group by odate, prov_id, page_type
	) b on  a.odate=b.odate and a.prov_id=b.prov_id and a.page_type = b.page_type;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

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
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,2,count(n.user_name)
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
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,99,COALESCE(sum(n.wlan_time),0),COALESCE(sum(n.input_octets+n.output_octets),0),count(n.user_name)
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
	delete from rpt_active_ua_type_month where to_date(odate,'yyyy-mm') = to_date(vi_dealdate,'yyyy-mm');

	--统计终端类型活跃用户数
	insert into rpt_active_ua_type_month(odate,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm'),
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,1,count(n.user_name)
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
	       (case when position('mobile' in n.user_domain) > 0 and upper(n.user_agent) != 'UA0047' then 1
		     when position('pc' in n.user_domain) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when n.user_agent is null or n.user_agent = '' then 4
	             else 0 end  ) ua_type,2,count(n.user_name)
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

CREATE OR REPLACE FUNCTION write_runlog(v_func_name text,v_log_desc text,v_status integer)
  RETURNS void AS
$BODY$

begin

INSERT INTO sys_run_log(
            func_name, log_desc, status, create_time)
    VALUES (v_func_name, v_log_desc, v_status, clock_timestamp());
--current_time is a timestamp with timezone.can not work here

end;
$BODY$
  LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION etl_rpt_auth_request_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_auth_request_day';
begin
	--按日期、省份、错误类型、acip统计错误数量到表rpt_cha_nasip_day
        perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete start',0);
	delete from rpt_auth_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert start',0);
	insert into rpt_auth_request_day(odate, prov_id, connected_num, blocked_num, bas_err_num)
	select t.odate,c.ac_prov_id as prov_id,sum(t.connected_num) as connected_num,-1 as blocked_num,sum(t.bas_err_num) as bas_err_num
	from (select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,ac_ip,
		sum(case when detail_info='认证请求被拒绝' then 1 else 0 end) as connected_num,-1 as blocked_num,
		--sum(case when detail_info='请求AUTH此链接已建立数' then 1 else 0 end) as connected_num,
		--sum(case when detail_info='请求AUTH有一个用户正在认证过程中，请稍后再试次数' then 1 else 0 end) as blocked_num,
		sum(case when detail_info='请求AUTH，上线BAS错误数' then 1 else 0 end) as bas_err_num
	from TEMP_NORMAL_LOGIN_REQUEST t
	--where detail_info in ('请求AUTH此链接已建立数','请求AUTH有一个用户正在认证过程中，请稍后再试次数','请求AUTH，上线BAS错误数')
	where detail_info in ('认证请求被拒绝','请求AUTH，上线BAS错误数')
		and not exists (select 1 from src_radius_log s where t.date_time = s.date_time and  t.user_name = s.user_name and t.ac_ip= s.nas_ip)
	group by ac_ip) t,sys_prov_acip_info c  
	where t.ac_ip = c.ac_ip group by t.odate,c.ac_prov_id;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_user_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm-dd
	
begin

	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	--清理当前统计日期下的数据
	delete from rpt_active_user_day where odate= to_date(vi_dealdate,'yyyy-mm-dd');

	--统计集团活跃用户数
	insert into rpt_active_user_day(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name
	) a;

	--统计使用地活跃用户数
	insert into rpt_active_user_day(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	--统计归属地活跃用户数
	insert into rpt_active_user_day(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_day(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,
		       (case when t.user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,user_type
	) a
	group by a.user_type;

	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,
		       (case when t.user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,t.node_id,user_type
	) a
	group by a.node_id,a.user_type;

	--统计登录方式维度的活跃用户数
	insert into rpt_active_user_day(odate,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.login_type,7,count(a.user_name)
	from(
		select t.user_name,
		       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,login_type
	) a
	group by a.login_type;

	--统计使用地、登录方式维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.login_type,8,count(a.user_name)
	from(
		select t.user_name,t.node_id,
		       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,t.node_id,login_type
	) a
	group by a.node_id,a.login_type;

	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,
	       (case when t.user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,99,COALESCE(sum(t.wlan_time),0) wlan_time,
	        COALESCE(sum(t.input_octets + t.output_octets),0) in_out_octets,count(t.user_name) use_num
	from src_usage t
	where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
	and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm-dd')
	group by to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,user_type,login_type;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	
begin

	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_active_user_month where to_date(odate,'yyyy-mm')= to_date(vi_dealdate,'yyyy-mm');

	--统计集团活跃用户数
	insert into rpt_active_user_month(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name
	) a;

	--统计使用地活跃用户数
	insert into rpt_active_user_month(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	--统计归属地活跃用户数
	insert into rpt_active_user_month(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,
		       (case when t.user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,user_type
	) a
	group by a.user_type;

	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,
		       (case when t.user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,user_type
	) a
	group by a.node_id,a.user_type;

	--统计登录方式维度的活跃用户数
	insert into rpt_active_user_month(odate,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.login_type,7,count(a.user_name)
	from(
		select t.user_name,
		       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,login_type
	) a
	group by a.login_type;

	--统计使用地、登录方式维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.login_type,8,count(a.user_name)
	from(
		select t.user_name,t.node_id,
		       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,login_type
	) a
	group by a.node_id,a.login_type;

	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,
	       (case when t.user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,99,COALESCE(sum(t.wlan_time),0) wlan_time,
	        COALESCE(sum(t.input_octets + t.output_octets),0) in_out_octets,count(t.user_name) use_num
	from src_usage t
	where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
	and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
	group by to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,user_type,login_type;
	
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
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
	group by odate,prov_id,err_reason;

	perform write_runlog(v_func_name,'function end',0);
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 1 end ) login_type, 
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 1 end ) login_type, 
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

CREATE OR REPLACE FUNCTION etl_rpt_step_status(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	--上线请求各个阶段成功失败数统计
	--阶段,取值1-7，说明如下：
	--1:portal首页请求
	--2:ac推送portal首页成功
	--3:用户上线申请
	--4:正常上线申请
	--5:challenge请求
	--6:auth请求
	--7:radius请求
	delete from rpt_step_status where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--1:portal首页请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,1 as step,success_num,pv_num-success_num as failed_num,0 as network_failed_num
	from rpt_apache_day where odate= to_date(vi_dealdate, 'yyyy-mm-dd');

	--2:ac推送portal首页成功
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,r.prov_id,2 as step,m.m_num,r.success_num-m.m_num as failed_num,0 as network_failed_num
	from rpt_apache_day r,
		(select p.ac_prov_id as prov_id,count(*) as m_num from src_monitor_log s,sys_prov_acip_info p where s.date_time between to_date(vi_dealdate, 'yyyy-mm-dd')
		 and  to_date(vi_dealdate, 'yyyy-mm-dd')+1 and s.ac_ip=p.ac_ip group by p.ac_prov_id) m
	where r.prov_id=m.prov_id and odate= to_date(vi_dealdate, 'yyyy-mm-dd');

	--3:用户上线申请
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,a.prov_id,3 as step,sum(a.success_total+b.success_total) as success_num,
		sum(a.failed_total+b.failed_total) as failed_num,sum(a.obs_failed+a.OBS_RESP_EXPIRED+a.AC_BAS_RESP_EXPIRED+a.AUTH_BAS_ERR+a.CHA_BAS_ERR
		+b.obs_failed+b.OBS_RESP_EXPIRED+b.AC_BAS_RESP_EXPIRED+b.AUTH_BAS_ERR+b.CHA_BAS_ERR) as network_failed_num
	from rpt_scan_day a,rpt_normal_request_day b 
	where a.odate=b.odate and a.prov_id=b.prov_id and a.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and b.odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	group by a.prov_id;

	--4:正常上线申请
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,4 as step,sum(success_total) as success_total,sum(failed_total) as failed_num,0 as network_failed_num
	from rpt_normal_request_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd') group by prov_id;

	--5:challenge请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,5 as step,n.success_num-(connected+rejected+blocked+no_acip_found
		+bas_err+timeout+acname_not_matched+dup_login) as success_total,(connected+rejected+blocked+no_acip_found
		+bas_err+timeout+acname_not_matched+dup_login) as failed_num,0 as network_failed_num
	from rpt_cha_request_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=4;
	
	--6:auth请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,6 as step,n.success_num - (connected_num+blocked_num+bas_err_num) as success_total,
		(connected_num+blocked_num+bas_err_num) as failed_num,0 as network_failed_num
	from rpt_auth_request_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=5;
	
	--7:radius请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,7 as step,n.success_num - (limit3+wrong_pwd+dns_not_found+eap_timeout) as success_total,
		(limit3+wrong_pwd+dns_not_found+eap_timeout) as failed_num,0 as network_failed_num 
	from rpt_radius_auth_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=6;
	

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
	if exists(select 1 from pg_partitions where lower(tablename)=lower('TEMP_ONLINE_USER_IP') and partitionname = v_partition_name) then
		execute ' alter table TEMP_ONLINE_USER_IP truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table TEMP_ONLINE_USER_IP add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;
	
	insert into TEMP_ONLINE_USER_IP(odate, prov_id, user_ip)
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

CREATE OR REPLACE FUNCTION etl_temp_cha_err(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	--把四种相关错误（包括:1：请求Challenge此链接已建立;2：请求Challenge被拒绝;3：认证请求被拒绝;4：请求Challenge有一个用户正在认证过程中，请稍后再试)
	--归类统计，其中第三种错误需要关联src_radius日志表
	delete from temp_cha_err where date_time between to_date(vi_dealdate, 'yyyy-mm-dd') and to_date(vi_dealdate, 'yyyy-mm-dd') + 1;

	insert into temp_cha_err(date_time, prov_id, user_name, err_type)
	select date_time, c.ac_prov_id as prov_id, user_name,case when detail_info='请求Challenge此链接已建立' then 1
					when detail_info='请求Challenge被拒绝' then 2
					when detail_info='请求Challenge有一个用户正在认证过程中，请稍后再试' then 4 end as err_type
	from TEMP_NORMAL_LOGIN_REQUEST t,sys_prov_acip_info c  
	where t.ac_ip = c.ac_ip  and detail_info in ('请求Challenge此链接已建立','请求Challenge被拒绝','请求Challenge有一个用户正在认证过程中，请稍后再试')
	union all
	select s.date_time,c.ac_prov_id as prov_id,s.user_name,3 as err_type
		from TEMP_NORMAL_LOGIN_REQUEST t,src_radius_log s,sys_prov_acip_info c  
	where t.ac_ip = c.ac_ip  and  t.date_time = s.date_time and  t.user_name = s.user_name and t.ac_ip= s.nas_ip
		and t.detail_info='认证请求被拒绝' and s.result like '%Checking LM%';
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;


DROP TABLE temp_login_request_success;

CREATE TABLE temp_login_request_success
(
  date_time timestamp without time zone, -- 日期时间
  user_name character varying(64), -- 用户名称
  user_domain character varying(64), -- 登录类型
  user_agent text -- 用户UA
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (date_time, user_name);
ALTER TABLE temp_login_request_success
  OWNER TO aidns;
COMMENT ON TABLE temp_login_request_success
  IS '登入认证请求“webauth_logon”，结果状态为“认证成功”的临时表，用于话单关联。  注意，不用去重';
COMMENT ON COLUMN temp_login_request_success.date_time IS '日期时间';
COMMENT ON COLUMN temp_login_request_success.user_name IS '用户名称';
COMMENT ON COLUMN temp_login_request_success.user_domain IS '登录类型';
COMMENT ON COLUMN temp_login_request_success.user_agent IS '用户UA';
