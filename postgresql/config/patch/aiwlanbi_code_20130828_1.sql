CREATE OR REPLACE FUNCTION etl_rpt_cboss_monitor_day(vi_dealdate text)
  RETURNS void AS
$BODY$
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
	select to_timestamp(a.arr[1]||' '||a.arr[2],'yyyy/mm/dd hh24:mi:ss') odate,a.bip_code,a.trans_id,a.biz_type,a.opr_code,a.user_name,a.sp_biz_code,a.user_status,
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
        ) a
        where date_trunc('day',odate)=to_date(vi_dealdate,'yyyy-mm-dd');

        perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;


