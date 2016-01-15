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
		select DATE_TIME, USER_NAME, user_ip,ac_ip, substr(op_type,1,100) as op_type, stype , err_type, substr(DETAIL_INFO,1,100) as DETAIL_INFO, USER_AGENT
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

CREATE OR REPLACE FUNCTION etl_radius_log_hour(vi_dealdate text)
  RETURNS void AS
$BODY$
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
	)b where length(result_type) <= 50 and length(AUTHEN_TYPE) <= 20;


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
$BODY$
  LANGUAGE plpgsql VOLATILE;


