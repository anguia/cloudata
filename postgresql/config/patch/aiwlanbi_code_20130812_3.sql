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
$BODY$
  LANGUAGE plpgsql VOLATILE;