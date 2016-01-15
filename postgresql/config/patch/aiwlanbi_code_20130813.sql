--
-- Name: check_ip(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION check_ip(vi_ip text) RETURNS inet
    AS $_$
declare
	is_ip boolean;
	result inet;
begin

	is_ip = (select vi_ip ~ E'^((25[0-5])|(2[0-4]\\d)|(1\\d\\d)|([1-9]\\d)|\\d)([.]((25[0-5])|(2[0-4]\\d)|(1\\d\\d)|([1-9]\\d)|\\d)){3}$');

	if is_ip = 't' then
		result = vi_ip :: inet;
	end if;

	return result;
end;
$_$
    LANGUAGE plpgsql;



--
-- Name: etl_apache_log_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_apache_log_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_apache_log_day';
begin
	---------天调用。 从IP段得到省份信息。

	---------临时表出来， 计算IP属于那个省份。
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert temp_userip_prov start',0);
	
	truncate table temp_userip_prov;
	insert into temp_userip_prov(user_ip, prov_id)
	select user_ip, case when b.prov_id is null then -1 else b.prov_id end as prov_id 
	from(
		select user_ip
		from temp_APACHE_LOG
		group by user_ip
	) a
	left join SYS_prov_ipseg_info b on  a.user_ip between b.start_ip and b.end_ip;

	perform write_runlog(v_func_name,'insert SRC_APACHE_LOG start',0);
	---------外部表入库， 入中间表
	insert into SRC_APACHE_LOG(ODATE, PROV_ID, USER_IP, PAGE_TYPE, STATUS_CODE, p_count)
	select ODATE, prov_id, a.USER_IP, PAGE_TYPE, STATUS_CODE, sum(p_count)
	from temp_APACHE_LOG a
	left join temp_userip_prov b on a.user_ip = b.user_ip
	where b.prov_id != -1
	group by  ODATE, prov_id, a.USER_IP, PAGE_TYPE, STATUS_CODE;
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_apache_log_hour(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_apache_log_hour(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_apache_log_hour';
begin
	-------分时间段调用， APACHE日志入库。

	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert temp_APACHE_LOG start',0);

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
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_monitor_log_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_monitor_log_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_monitor_log_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'insert TEMP_FIXED_PARAM_ATTACK start',0);
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

	perform write_runlog(v_func_name,'insert temp_monitor_log_1 start',0);

	truncate table temp_monitor_log_1;
	insert into TEMP_MONITOR_LOG_1(DATE_TIME, USER_NAME, USER_IP, AC_IP, stype, err_type, DETAIL_INFO, USER_AGENT)
	select a.DATE_TIME, a.USER_NAME, a.USER_IP, a.AC_IP, stype, err_type, a.DETAIL_INFO, a.USER_AGENT
	from  src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and not exists(select * from (
		select user_ip,	ac_ip
		from TEMP_FIXED_PARAM_ATTACK
		where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
		group by user_ip, ac_ip)b where date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd') and a.ac_ip=b.ac_ip and a.user_ip=b.user_ip);	

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
	insert into TEMP_LOGIN_REQUEST_SUCCESS(date_time,user_name,user_domain,user_agent)
	select date_time,case when position('@' in user_name)>1 then substring(user_name,1,position('@' in user_name)-1) else user_name end as user_name,
		case when position('@' in user_name)>1 then substring(user_name,position('@' in user_name)+1) else null end as user_domain
		,user_agent
	from src_MONITOR_LOG a
	where op_type='webauth_logon' and date_trunc('day', DATE_TIME) = to_date(vi_dealDate, 'yyyy-mm-dd')
	and detail_info='认证成功';

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_monitor_log_hour(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_monitor_log_hour(vi_dealdate text) RETURNS void
    AS $$
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
	else
		execute ' alter table src_MONITOR_LOG truncate partition ' || v_partition_name || ';';
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
$$
    LANGUAGE plpgsql;



--
-- Name: etl_radius_log_hour(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_radius_log_hour(vi_dealdate text) RETURNS void
    AS $$
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
	)b;


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
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_active_ua_type_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_day(vi_dealdate text) RETURNS void
    AS $$
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
	       (case when n.user_name ~ '[0-9]{11}' then 1
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
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_active_ua_type_month(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_month(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_active_ua_type_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_active_ua_type_month start',0);
	
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm'); 

	--清理当前统计日期下的数据
	delete from rpt_active_ua_type_month where to_date(odate,'yyyy-mm') = to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_active_ua_type_month(stat_type=1) start',0);
	
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
	group by to_date(vi_dealDate,'yyyy-mm'),ua_type ;

	perform write_runlog(v_func_name,'insert rpt_active_ua_type_month(stat_type=2) start',0);
	
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
	group by to_date(vi_dealDate,'yyyy-mm'),n.node_id,ua_type ;

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_active_user_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_active_user_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_day text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_active_user_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_active_user_day start',0);
	
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	--清理当前统计日期下的数据
	delete from rpt_active_user_day where odate= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=1) start',0);
	
	--统计集团活跃用户数
	insert into rpt_active_user_day(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_day(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=3) start',0);
	
	--统计归属地活跃用户数
	insert into rpt_active_user_day(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=4) start',0);
	
	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=5) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,user_type
	) a
	group by a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=6) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,user_type
	) a
	group by a.node_id,a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=7) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,login_type
	) a
	group by a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=8) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,login_type
	) a
	group by a.node_id,a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=9) start',0);
	
	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=99) start',0);
	
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
	where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
	group by to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,user_type,login_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_active_user_month(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_active_user_month(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_active_user_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_active_user_month start',0);
	
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_active_user_month where to_date(odate,'yyyy-mm')= to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=1) start',0);
	
	--统计集团活跃用户数
	insert into rpt_active_user_month(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_month(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=3) start',0);
	
	--统计归属地活跃用户数
	insert into rpt_active_user_month(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=4) start',0);
	
	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=5) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,user_type
	) a
	group by a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=6) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,user_type
	) a
	group by a.node_id,a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=7) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,login_type
	) a
	group by a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=8) start',0);
	
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
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,login_type
	) a
	group by a.node_id,a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=9) start',0);
	
	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=99) start',0);
	
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
	where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
	group by to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,user_type,login_type;

	perform write_runlog(v_func_name,'function end',0);
end;

$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_apache_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_apache_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_apache_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	---------------按日期、省份统计， 推送成功数、PV、UV、潜在用户数、访问介绍页面IP数
	perform write_runlog(v_func_name,'delete rpt_apache_day start',0);
	delete from rpt_apache_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	
	perform write_runlog(v_func_name,'insert rpt_apache_day start',0);
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
		select odate, prov_id, sum(case when STATUS_CODE = 200 and page_type = 1 then 1 else 0 end) as UV_NUM
		from (
			select odate, prov_id, user_ip, page_type, STATUS_CODE
			from SRC_APACHE_LOG
			where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
			group by odate, prov_id, user_ip, page_type, STATUS_CODE
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
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_apache_month(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_apache_month(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_apache_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------统计Portal访问相关信息, 月报表统计

	perform write_runlog(v_func_name,'delete rpt_apache_month start',0);
	delete from rpt_apache_month where odate = to_date(vi_dealdate, 'yyyy-mm'); 

	perform write_runlog(v_func_name,'insert rpt_apache_month start',0);
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
			, sum(intro_page_num ) as INTRO_PAGE_NUM
			, sum(UV_NUM) as UV_NUM
		from (
			select date_trunc('month', odate) :: date as m_odate, prov_id, user_ip
				, max(potential_user)  as POTENTIAL_USER_NUM
				, max(intro_page) as INTRO_PAGE_NUM
				, max(uv_flag)  as UV_NUM
				, count(1) as u_count, sum(potential_user) as u_sum
			from SRC_APACHE_MONTH_LOG
			where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm')
			group by m_odate, prov_id, user_ip
		) a
		group by m_odate, prov_id
	) b on a.m_odate = b.m_odate and a.prov_id = b.prov_id;	

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_auth_request_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_auth_request_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_auth_request_day';
begin
	--按日期、省份、错误类型、acip统计错误数量到表rpt_cha_nasip_day
        perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_auth_request_day start',0);
	delete from rpt_auth_request_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_auth_request_day start',0);
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
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_cboss_monitor_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_cboss_monitor_day(vi_dealdate text) RETURNS void
    AS $$
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
	select to_timestamp(a.arr[1]||' '||a.arr[2],'yyyy/mm/dd hh24:mi:ss'),a.bip_code,a.trans_id,a.biz_type,a.opr_code,a.user_name,a.sp_biz_code,a.user_status,
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
		       trim(part20)::integer home_prov
		from ext_cboss_log 
		where trim(part2) = 'BIP2B147'
        ) a;

        perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_cha_cookie_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_cha_cookie_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_cha_cookie_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_cha_cookie_day start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	--清理当前统计日期下的数据
	delete from rpt_cha_cookie_day where odate = to_date(vi_dealDate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_cha_cookie_day start',0);
	
	--统计“请求Challenge有一个用户正在认证过程中错误cookie认证分析”
	insert into rpt_cha_cookie_day (odate,prov_id,err_num,cookie_num)
	select to_date(vi_dealDate, 'yyyy-mm-dd'), a.prov_id,count(distinct a.user_name) err_num,sum(case when b.bms_user_name is null then 0 else 1 end) cookie_num
	from (
		select t.prov_id,t.user_name
		from temp_cha_err t
		where t.err_type = 4
		and date_trunc('day', t.date_time) = to_date(vi_dealDate,'yyyy-mm-dd')
	) a 
	left join (

		select t.bms_user_name
		from src_wlan_user_cookie t 
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
	) b on a.user_name = b.bms_user_name
	group by to_date(vi_dealDate, 'yyyy-mm-dd'), a.prov_id;

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_cha_err_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_cha_err_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_cha_err_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_cha_err_day start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	--清理当前统计日期下的数据
	delete from rpt_cha_err_day where odate = to_date(vi_dealDate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_cha_err_day start',0);
	
	--统计“三种错误账号已在线分析”数量
	insert into rpt_cha_err_day (odate,prov_id,err_type,err_num)
	select to_date(vi_dealDate, 'yyyy-mm-dd'),b.prov_id,b.err_type,count(1) err_num
	from (
		select t.user_name
		from src_usage t
		where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
	) a,
	(
		select t.prov_id,t.err_type,t.user_name
		from temp_cha_err t
		where date_trunc('day', t.date_time) = to_date(vi_dealDate,'yyyy-mm-dd')
		and t.err_type in (1,2,3)
	) b
	where a.user_name = b.user_name
	group by to_date(vi_dealDate, 'yyyy-mm-dd'),b.prov_id,b.err_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_cha_request_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_cha_request_day(vi_dealdate text) RETURNS void
    AS $$
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
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_hotspot_usage(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_hotspot_usage(vi_dealdate text) RETURNS void
    AS $$

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
				     when t.user_name ~ '[0-9]{11}' then 1 
				     else 4 end) user_type,
			       t.nas_identifier
			from src_usage t
			where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')			
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
				     when t.user_name ~ '[0-9]{11}' then 1 
				     else 4 end) user_type,
			       t.nas_ip
			from src_usage t
			where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
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
			where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd')
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

$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_new_active_user_month(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_new_active_user_month(vi_dealdate text) RETURNS void
    AS $$

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
	       (case when t.user_name ~ '[0-9]{11}' then 1
	             when position('EDU.' in upper(a.user_name)) > 0 then 2
	             when position('STARBUCKS' in upper(a.user_name)) > 0 then 3
	             else 4 end),count(a.user_name)
	from (
		select t.node_id,
		       t.user_name,
		       t.customer_type	       
		from src_usage t
		where date_trunc('month', t.time_stamp) = to_date(vi_dealdate,'yyyy-mm')		
		group by t.node_id,t.user_name,t.customer_type
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
	      (case when t.user_name ~ '[0-9]{11}' then 1
	             when position('EDU.' in upper(a.user_name)) > 0 then 2
	             when position('STARBUCKS' in upper(a.user_name)) > 0 then 3
	             else 4 end);
	             
	perform write_runlog(v_func_name,'function end',0);
end;

$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_no_subscription_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_no_subscription_day(vi_dealdate text) RETURNS void
    AS $$
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
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by odate, prov_id;
	
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_normal_request_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_normal_request_day(vi_dealdate text) RETURNS void
    AS $$
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
            from (select ac_ip,(case when user_name ~ '[0-9]{11}' then 1
	                     when position('EDU.' in upper(user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(user_name)) > 0 then 3
	                     else 4 end) user_type, 
		       (case when position('WEB.PC' in upper(user_name)) > 0 then 1
		       			 when position('WEB.MOBILE' in upper(user_name)) > 0 then 2
		             when position('CTL.PC' in upper(user_name)) > 0 then 3
		             when position('CTL.MOBILE' in upper(user_name)) > 0 then 4
		             else 0 end ) login_type, 
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
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected,
		             case when detail_info='认证成功' then 1 else 0 end as success
	from temp_normal_login_request) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_online_user_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_online_user_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_online_user_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete RPT_ONLINE_USER_DAY start',0);
	--------上线用户IP数日结果统计
	delete from RPT_ONLINE_USER_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert RPT_ONLINE_USER_DAY start',0);
	insert into RPT_ONLINE_USER_DAY(odate, prov_id, USER_IP_NUM)
	select odate, prov_id, count(1) USER_IP_NUM
	from TEMP_ONLINE_USER_IP where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by odate, prov_id;
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_online_user_month(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_online_user_month(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_online_user_month';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete RPT_ONLINE_USER_MONTH start',0);
	--------上线用户IP数月结果统计
	delete from RPT_ONLINE_USER_MONTH where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm');

	perform write_runlog(v_func_name,'insert RPT_ONLINE_USER_MONTH start',0);
	insert into RPT_ONLINE_USER_MONTH(odate, prov_id, USER_IP_NUM)
	select date_trunc('month', odate) as odate_1, prov_id, count(1) USER_IP_NUM
	from TEMP_ONLINE_USER_IP
	where date_trunc('month', odate) = to_date(vi_dealdate, 'yyyy-mm')
	group by odate_1, prov_id;
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_page_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_page_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_rpt_page_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------APCHE日志, 页面统计
	perform write_runlog(v_func_name,'delete rpt_page_day start',0);
	delete from rpt_page_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_page_day start',0);
	insert into rpt_page_day(odate, prov_id, PAGE_TYPE, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.odate, a.prov_id, a.PAGE_TYPE, sum(potential_user) as POTENTIAL_USER_NUM
		, INTRO_PAGE_NUM
	from (select odate, prov_id, page_type,sum(p_count) as INTRO_PAGE_NUM
		from SRC_APACHE_LOG 
		where page_type <> -1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id, page_type
	)a
	left join SRC_APACHE_MONTH_LOG b on a.prov_id = b.prov_id and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by a.odate, a.prov_id, a.page_type, INTRO_PAGE_NUM;

	perform write_runlog(v_func_name,'delete RPT_INTRO_PAGE_DAY start',0);
	-------------分省份访问介绍页面的IP
	delete from RPT_INTRO_PAGE_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');
	perform write_runlog(v_func_name,'insert RPT_INTRO_PAGE_DAY start',0);
	insert into RPT_INTRO_PAGE_DAY(odate, prov_id, USER_IP)
	select odate, prov_id, USER_IP
	from SRC_APACHE_MONTH_LOG
	where potential_user = 1 and intro_page = 1 and odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	group by odate, prov_id, user_ip;

	-------------分页访问介绍页面的IP日累计表
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	
	if exists(select 1 from pg_partitions where lower(tablename)=lower('rpt_apache_log_add_day') and partitionname = v_partition_name) then
		execute ' alter table rpt_apache_log_add_day truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table rpt_apache_log_add_day add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert rpt_apache_log_add_day start',0);
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

	perform write_runlog(v_func_name,'insert TEMP_APACHE_LOG_ADD_DAY start',0);
	insert into TEMP_APACHE_LOG_ADD_DAY(odate, user_ip, prov_id, page_type, p_count)
	select odate, user_ip, prov_id, page_type, p_count
	from SRC_APACHE_LOG 
	where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_page_month(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_page_month(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_page_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	--------统计Portal访问相关信息, 月报表统计
	
	truncate table temp_RPT_PAGE_MONTH;
	perform write_runlog(v_func_name,'insert temp_RPT_PAGE_MONTH start',0);
	insert into temp_RPT_PAGE_MONTH(odate, prov_id, user_ip, PAGE_TYPE,p_count)
	select a.m_odate, a.prov_id, user_ip, PAGE_TYPE, p_count
	from (
		select date_trunc('month', odate) :: date as m_odate, prov_id, PAGE_TYPE, user_ip, sum(p_count) as p_count
		from TEMP_APACHE_LOG_ADD_DAY 
		where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm') 
		group by m_odate, prov_id, PAGE_TYPE, user_ip
	) a;

	perform write_runlog(v_func_name,'delete RPT_PAGE_MONTH start',0);
	delete from RPT_PAGE_MONTH where odate = to_date(vi_dealdate, 'yyyy-mm'); 

	perform write_runlog(v_func_name,'insert RPT_PAGE_MONTH start',0);
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
	) b on a.odate=b.odate and a.prov_id=b.prov_id and a.page_type = b.page_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_pwd_err_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_pwd_err_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_pwd_err_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_pwd_err_day start',0);
	delete from rpt_pwd_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_pwd_err_day start',0);
	--根据省份，统计部分成功数和完全失败的数
	--此处“其他错误”已经包括“其他错误(OBS)”+“其他错误（PORTAL）”+“其他错误（AC）”
	insert into rpt_pwd_err_day(odate, prov_id, part_failed_num, all_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,
		sum(case when all_num<>fail_num then 1 else 0 end) as part_failed_num,
		sum(case when all_num=fail_num then 1 else 0 end) as all_failed_num
	 from (select ac_ip,count(*) as all_num,
		sum(case when detail_info in ('AC名称不匹配','OBS访问失败','用户没有订购业务 ','用户密码错误','用户状态错误',
			'自动认证已过期(cookie)','动态密码有效期过期','用户上线且使用同一用户名和IP重复登录','用户先上线,然后用另一名字在同一客户机器再认证',
			'认证请求被拒绝','用户没有可用时长','用户卡无效','读取OBS响应包超时','接收AC/BAS响应包超时','其他错误','请求auth，上线BAS错误',
			'请求Challenge，上线BAS错误','请求Challenge被拒绝','请求Challenge此链接已建立','请求Challenge有一个用户正在认证过程中，请稍后再试',
			'认证请求被拒绝(星巴克）') then 1 else 0 end) as fail_num
	from temp_normal_login_request group by ac_ip) t,SYS_prov_ipseg_info c  
	where t.ac_ip between c.start_ip and c.end_ip group by c.prov_id;
	perform write_runlog(v_func_name,'function end',0);

end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_radius_auth_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_radius_auth_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_radius_auth_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete RPT_RADIUS_AUTH_DAY start',0);
	--------radius认证分析表
	delete from RPT_RADIUS_AUTH_DAY where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert RPT_RADIUS_AUTH_DAY start',0);
	insert into RPT_RADIUS_AUTH_DAY(odate, prov_id, ac_ip,LIMIT3, WRONG_PWD, DNS_NOT_FOUND, EAP_TIMEOUT)	
	select odate, prov_id, ac_ip,sum(LIMIT3), sum(WRONG_PWD), sum(DNS_NOT_FOUND), sum(EAP_TIMEOUT) 
	from ( 
		select date_trunc('day', a.date_time) as odate, ac_ip, sum(case when result_type = 2 then 1 else 0 end ) as LIMIT3
			, sum(case when result_type = 3 then 1 else 0 end ) as WRONG_PWD
			, sum(case when result_type = 1 then 1 else 0 end ) as DNS_NOT_FOUND
			, sum(case when result_type = 4 then 1 else 0 end ) as EAP_TIMEOUT
		from (
			select date_time, user_name,  ac_ip 
			from TEMP_NORMAL_LOGIN_REQUEST
			where detail_info = '认证请求被拒绝'
			group by date_time, user_name,  ac_ip 
		) a, 
		(
			select date_time, user_name, nas_ip,
				case when result like '%Can''t Found Roaming Domain%' then 1
					when result like '%Checking LM%' then 2
					when result like '%Authen Attrib(ai-Service-Password) Check Error%' then 3
					when result like '%No Service response%' then 4 else 0 end as result_type
			from SRC_RADIUS_LOG
		) b
		where a.date_time = b.date_time and  a.user_name = b.user_name and a.ac_ip= b.nas_ip and result_type <>0
		group by odate, ac_ip
	) tmp ,SYS_prov_ipseg_info c  
	where tmp.ac_ip between start_ip and end_ip
	group by odate, prov_id,ac_ip;
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_scan_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_scan_day(vi_dealdate text) RETURNS void
    AS $$
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
		             else 0 end ) login_type, 
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
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected
	from temp_fixed_param_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;

	perform write_runlog(v_func_name,'insert rpt_scan_day2 start',0);
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
		             else 0 end ) login_type, 
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
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected
	from temp_ip_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;

	perform write_runlog(v_func_name,'insert rpt_scan_day3 start',0);
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
		             else 0 end ) login_type, 
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
		             case when detail_info='认证请求被拒绝' and stype='PT115' then 1 else 0 end as auth_rejected,
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
		             case when detail_info='认证请求被拒绝' and stype='PT999' then 1 else 0 end as starbuck_auth_rejected
	from temp_account_attack) t1 group by ac_ip,user_type,login_type) t2,sys_prov_acip_info s
	where t2.ac_ip=s.ac_ip;
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_scan_type_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_scan_type_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_scan_type_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_scan_type_day start',0);
	delete from rpt_scan_type_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_scan_type_day start',0);
	--扫号软件信息统计，按照日期，省份，扫好类型，数量统计
	insert into rpt_scan_type_day(odate, prov_id, scan_type, err_type, err_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 1 as err_type,acname_not_matched as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 2 as err_type,obs_failed as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 3 as err_type,other_obs_failed as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 4 as err_type,no_subscription as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 5 as err_type,wrong_pwd as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 6 as err_type,wrong_status as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 7 as err_type,other_portal_failed as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 8 as err_type,auto_expired as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 9 as err_type,pwd_expired as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 10 as err_type,dup_ip_user as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 11 as err_type,dup_auth as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 12 as err_type,auth_rejected as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 13 as err_type,no_wlan_time as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 14 as err_type,card_expired as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 15 as err_type,obs_resp_expired as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 16 as err_type,ac_bas_resp_expired as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 17 as err_type,other_failed as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 18 as err_type,auth_bas_err as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 19 as err_type,cha_bas_err as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 20 as err_type,cha_rejected as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 21 as err_type,cha_connected as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 22 as err_type,auth_blocked as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 23 as err_type,starbuck_auth_rejected as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	union all
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id, scan_type, 24 as err_type,success_total as err_num  from rpt_scan_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd');
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_status_err_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_status_err_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_status_err_day';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_status_err_day start',0);
	delete from rpt_status_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_status_err_day start',0);
	--用户状态错误分析,从订阅表得来
	insert into rpt_status_err_day(odate, prov_id, default_lock_num, flow_lock_num, cancel_lock_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,bms_node_id as prov_id,sum(case when BMS_SUBSCRIPTION_STATUS=3 then 1 else 0 end) as default_lock_num,
		sum(case when BMS_SUBSCRIPTION_STATUS=1 then 1 else 0 end) as flow_lock_num,
		sum(case when BMS_SUBSCRIPTION_STATUS=2 then 1 else 0 end) as cancel_lock_num
	from src_subscription
	where BMS_SUBSCRIPTION_STATUS in (1,2,3) and bms_create_time between to_date(vi_dealdate, 'yyyy-mm-dd') and to_date(vi_dealdate, 'yyyy-mm-dd') + 1
	group by bms_node_id;
	perform write_runlog(v_func_name,'function end',0);
	
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_step_status(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_step_status(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_step_status';
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
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_step_status start',0);
	delete from rpt_step_status where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_step_status1 start',0);
	--1:portal首页请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,1 as step,success_num,pv_num-success_num as failed_num,0 as network_failed_num
	from rpt_apache_day where odate= to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_step_status2 start',0);
	--2:ac推送portal首页成功
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,r.prov_id,2 as step,m.m_num,r.success_num-m.m_num as failed_num,0 as network_failed_num
	from rpt_apache_day r,
		(select p.ac_prov_id as prov_id,count(*) as m_num from src_monitor_log s,sys_prov_acip_info p where s.date_time between to_date(vi_dealdate, 'yyyy-mm-dd')
		 and  to_date(vi_dealdate, 'yyyy-mm-dd')+1 and s.ac_ip=p.ac_ip group by p.ac_prov_id) m
	where r.prov_id=m.prov_id and odate= to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_step_status3 start',0);
	--3:用户上线申请
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,a.prov_id,3 as step,sum(a.success_total+b.success_total) as success_num,
		sum(a.failed_total+b.failed_total) as failed_num,sum(a.obs_failed+a.OBS_RESP_EXPIRED+a.AC_BAS_RESP_EXPIRED+a.AUTH_BAS_ERR+a.CHA_BAS_ERR
		+b.obs_failed+b.OBS_RESP_EXPIRED+b.AC_BAS_RESP_EXPIRED+b.AUTH_BAS_ERR+b.CHA_BAS_ERR) as network_failed_num
	from rpt_scan_day a,rpt_normal_request_day b 
	where a.odate=b.odate and a.prov_id=b.prov_id and a.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and b.odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	group by a.prov_id;

	perform write_runlog(v_func_name,'insert rpt_step_status4 start',0);
	--4:正常上线申请
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,4 as step,sum(success_total) as success_total,sum(failed_total) as failed_num,0 as network_failed_num
	from rpt_normal_request_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd') group by prov_id;

	perform write_runlog(v_func_name,'insert rpt_step_status5 start',0);
	--5:challenge请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,5 as step,n.success_num-(connected+rejected+blocked+no_acip_found
		+bas_err+timeout+acname_not_matched+dup_login) as success_total,(connected+rejected+blocked+no_acip_found
		+bas_err+timeout+acname_not_matched+dup_login) as failed_num,0 as network_failed_num
	from rpt_cha_request_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=4;

	perform write_runlog(v_func_name,'insert rpt_step_status6 start',0);	
	--6:auth请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,6 as step,n.success_num - (connected_num+blocked_num+bas_err_num) as success_total,
		(connected_num+blocked_num+bas_err_num) as failed_num,0 as network_failed_num
	from rpt_auth_request_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=5;

	perform write_runlog(v_func_name,'insert rpt_step_status7 start',0);
	--7:radius请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,7 as step,n.success_num - (limit3+wrong_pwd+dns_not_found+eap_timeout) as success_total,
		(limit3+wrong_pwd+dns_not_found+eap_timeout) as failed_num,0 as network_failed_num 
	from rpt_radius_auth_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=6;
	
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_subscription_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_subscription_day(vi_dealdate text) RETURNS void
    AS $$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_subscription_day';
begin

	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_subscription_day start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_subscription_day where odate = to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_subscription_day start',0);
	
	--统计用户套餐订购情况
	insert into rpt_subscription_day (odate,bms_node_id,user_suite_type,user_num,new_user_num)	
	select to_date(vi_dealdate,'yyyy-mm-dd'),
	       n.prov_id,
	       n.user_suit_type,
	       COALESCE(n.user_num,0),
	       COALESCE(n.new_user_num,0)
	from (
	select prov.prov_id,d.user_suit_type,d.user_num,d.new_user_num
	from sys_prov_info prov
	left join (

	--统计公共用户包时套餐，校园用户包时套餐
	select t.bms_node_id,
	       (case when t.bms_product_id = 22 then 1 
	             when t.bms_product_id = 81 then 2
		     when t.bms_product_id = 82 then 3
		     when t.bms_product_id = 83 then 4
		     when t.bms_product_id = 16 or t.bms_product_id = 19 then 5
		     when t.bms_product_id = 17 or t.bms_product_id = 20 then 6
		     when t.bms_product_id = 18 or t.bms_product_id = 21 then 7
		     when t.bms_product_id = 84 then 8
		     when t.bms_product_id = 33 or t.bms_product_id = 40 then 12
		     when t.bms_product_id = 34 or t.bms_product_id = 41 then 13
		     when t.bms_product_id = 35 or t.bms_product_id = 42 then 14
		     else 0 end) user_suit_type,count(t.bms_user_name) user_num	,
	        sum(case when t.bms_create_time >= to_timestamp(vi_dealdate,'yyyy-mm-dd') then 1 else 0 end) new_user_num
	from src_subscription t
	where t.bms_create_time <to_timestamp(v_end_date,'yyyy-mm-dd')
	and t.bms_node_id != 0
	and t.bms_subscription_status = 0
	group by t.bms_node_id,user_suit_type

	union all

	--统计公共用户包流量套餐
	select t.bms_node_id,
	       (case when p.package_name ='10元自动认证套餐' then 9
	             when p.package_name ='20元自动认证套餐' then 10
	             when p.package_name ='50元自动认证套餐' then 11
	             else 0 end) user_suit_type,count(t.bms_user_name) user_num,
	       sum(case when p.time_stamp >=to_timestamp(vi_dealdate,'yyyy-mm-dd') then 1 else 0 end) new_user_num
	from src_subscription t,src_wlan_package p
	where p.time_stamp <to_timestamp(v_end_date,'yyyy-mm-dd') 
	and t.bms_user_name = p.bms_user_name 
	and t.bms_product_id != 32
	group by t.bms_node_id,user_suit_type
	) d on prov.prov_id = d.bms_node_id) n ;

	perform write_runlog(v_func_name,'function end',0);
end;

$$
    LANGUAGE plpgsql;



--
-- Name: etl_rpt_wlan_auth_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_rpt_wlan_auth_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_rpt_wlan_auth_day';
begin
	----------WLAN认证阶段分析
	perform write_runlog(v_func_name,'delete RPT_WLAN_AUTH_DAY start',0);
	delete from RPT_WLAN_AUTH_DAY where odate  = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert RPT_WLAN_AUTH_DAY start',0);
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
			where DATE_TIME :: date  = to_date(vi_dealdate, 'yyyy-mm-dd') and op_type = 'webauth_logon'
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

	perform write_runlog(v_func_name,'function end',0);
end

$$
    LANGUAGE plpgsql;



--
-- Name: etl_src_apache_month_log_day(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_src_apache_month_log_day(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_src_apache_month_log_day';
begin

	perform write_runlog(v_func_name,'function start',0);
	--------统计apcher 潜在用户（月累计， 每天聚合）
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if exists(select 1 from pg_partitions where lower(tablename)=lower('src_apache_month_log') and partitionname = v_partition_name) then
		execute ' alter table src_apache_month_log truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table src_apache_month_log add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert SRC_APACHE_MONTH_LOG start',0);
	---------所有的的USER_IP 需要进入 SRC_APACHE_MONTH_LOG表。
	insert into SRC_APACHE_MONTH_LOG(odate, prov_id, user_ip, POTENTIAL_USER, INTRO_PAGE, uv_flag)
	select a.odate, a.prov_id, a.user_ip, max(case when a.status_code = 200 and b.user_ip is null then 1 else 0 end) as POTENTIAL_USER
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

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_src_subscription(); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_src_subscription() RETURNS void
    AS $$
declare
	v_func_name text:='etl_src_subscription';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'truncate SRC_SUBSCRIPTION start',0);
	
	--清空订购表源表数据
	truncate table SRC_SUBSCRIPTION;

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

$$
    LANGUAGE plpgsql;



--
-- Name: etl_src_usage(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_src_usage(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_partition_name text; --表分区名称
	v_func_name text:='etl_src_usage';
begin
	perform write_runlog(v_func_name,'function start',0);
		
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');

	--检查表分区是否存在，不存在则新建，存在则删除
	if exists(select 1 from pg_partitions where lower(tablename)=lower('SRC_USAGE') and partitionname = v_partition_name) then
		perform write_runlog(v_func_name,'truncate SRC_USAGE partition start',0);
		execute ' alter table SRC_USAGE truncate partition ' || v_partition_name || ';';
	else 
		perform write_runlog(v_func_name,'add SRC_USAGE partition start',0);
		execute ' alter table SRC_USAGE add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert SRC_USAGE start',0);
	
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
	from ext_usage t
	where to_date(t.time_stamp,'yyyy-mm-dd') = to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'function end',0);
end

$$
    LANGUAGE plpgsql;



--
-- Name: etl_src_wlan_package(); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_src_wlan_package() RETURNS void
    AS $$
declare
	v_func_name text:='etl_src_wlan_package';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'truncate src_wlan_package start',0);
	
	--清空wlan资源叠加包源表数据
	truncate table src_wlan_package;

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

$$
    LANGUAGE plpgsql;



--
-- Name: etl_src_wlan_user_cookie(); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_src_wlan_user_cookie() RETURNS void
    AS $$
declare
	v_func_name text:='etl_src_wlan_user_cookie';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'truncate src_wlan_user_cookie start',0);
	
	--清空wlan用户cookie记录源表
	truncate table src_wlan_user_cookie;

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

$$
    LANGUAGE plpgsql;



--
-- Name: etl_temp_cha_err(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_temp_cha_err(vi_dealdate text) RETURNS void
    AS $$
declare
	v_func_name text:='etl_temp_cha_err';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete temp_cha_err start',0);
	--把四种相关错误（包括:1：请求Challenge此链接已建立;2：请求Challenge被拒绝;3：认证请求被拒绝;4：请求Challenge有一个用户正在认证过程中，请稍后再试)
	--归类统计，其中第三种错误需要关联src_radius日志表
	delete from temp_cha_err where date_time between to_date(vi_dealdate, 'yyyy-mm-dd') and to_date(vi_dealdate, 'yyyy-mm-dd') + 1;

	perform write_runlog(v_func_name,'insert temp_cha_err start',0);
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
	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: etl_temp_online_user_ip(text); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION etl_temp_online_user_ip(vi_dealdate text) RETURNS void
    AS $$
declare
	v_end_date text;
	v_partition_name text;
	v_func_name text:='etl_temp_online_user_ip';
begin
	perform write_runlog(v_func_name,'function start',0);
	
	--------统计MONITOR日志中在线用户数
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');
	if exists(select 1 from pg_partitions where lower(tablename)=lower('TEMP_ONLINE_USER_IP') and partitionname = v_partition_name) then
		execute ' alter table TEMP_ONLINE_USER_IP truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table TEMP_ONLINE_USER_IP add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	perform write_runlog(v_func_name,'insert TEMP_ONLINE_USER_IP start',0);
	insert into TEMP_ONLINE_USER_IP(odate, prov_id, user_ip)
	select odate, case when b.prov_id is null then -1 else prov_id end, user_ip
	from (
		select DATE_TIME:: date as odate, user_ip
		from SRC_MONITOR_LOG
		where op_type='webauth_logon' and user_ip is not null and DATE_TIME :: date = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, user_ip
	) a
	left join SYS_prov_ipseg_info b on a.user_ip between b.start_ip and b.end_ip;

	perform write_runlog(v_func_name,'function end',0);
end;
$$
    LANGUAGE plpgsql;



--
-- Name: fn_subscription(text, text[], text[]); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION fn_subscription(vi_user_name text, vi_m_tel text[], vi_nm_tel text[]) RETURNS integer
    AS $$
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
	if v_tel_tmp ~ '[0-9]{11}' then
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
$$
    LANGUAGE plpgsql;



--
-- Name: write_runlog(text, text, integer); Type: FUNCTION; Schema: public; Owner: aidns
--

CREATE OR REPLACE FUNCTION write_runlog(v_func_name text, v_log_desc text, v_status integer) RETURNS void
    AS $$

begin

INSERT INTO sys_run_log(
            func_name, log_desc, status, create_time)
    VALUES (v_func_name, v_log_desc, v_status, clock_timestamp());

end;
$$
    LANGUAGE plpgsql;
