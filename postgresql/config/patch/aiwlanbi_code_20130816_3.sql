-- Function: etl_rpt_page_day(text)

-- DROP FUNCTION etl_rpt_page_day(text);

CREATE OR REPLACE FUNCTION etl_rpt_page_day(vi_dealdate text)
  RETURNS void AS
$BODY$
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
	select a.odate, a.prov_id, a.page_type, POTENTIAL_USER_NUM, INTRO_PAGE_NUM
	from (
		select  a.odate, a.prov_id, a.page_type,sum(p_count) as INTRO_PAGE_NUM
		from SRC_APACHE_LOG a
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd') and a.page_type != -1
		group by a.odate, a.prov_id, a.page_type
	)a
	left join (
		select tmp.odate, tmp.prov_id, tmp.page_type, sum(case when b.user_ip is null then 1 else 0 end) as POTENTIAL_USER_NUM
		from (
			select  a.odate, a.prov_id, a.page_type, user_ip
			from SRC_APACHE_LOG a
			where odate = to_date(vi_dealdate, 'yyyy-mm-dd') and a.page_type != -1
			group by a.odate, a.prov_id, a.page_type, user_ip
		) tmp
		left join temp_online_user_ip b on tmp.user_ip = b.user_ip and b.odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by tmp.odate, tmp.prov_id, tmp.page_type
	)b on a.prov_id = b.prov_id and a.page_type = b.page_type;

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
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;