ALTER TABLE rpt_cboss_monitor_day ADD COLUMN deal_time numeric default  0;
CREATE EXTERNAL TABLE ext_cboss_do_log
(
  part1 text,
  part2 text,
  part3 text,
  part4 text,
  part5 text,
  part6 text,
  part7 text,
  part8 text,
  part9 text,
  part10 text,
  part11 text,
  part12 text,
  part13 text,
  part14 text,
  part15 text,
  part16 text,
  part17 text,
  part18 text,
  part19 text,
  part20 text
)
 LOCATION (
    'gpfdist://10.3.3.138:8004/domonitor*'
)
 FORMAT 'text' (delimiter '{' null E'\\N' escape E'\\' fill missing fields)
ENCODING 'UTF8'
LOG ERRORS INTO err_ext_cboss_do_log SEGMENT REJECT LIMIT 200000000 ROWS;

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

	perform write_runlog(v_func_name,'insert rpt_cboss_monitor_day from ext_cboss_log start',0);
	
	--从ext_cboss_log导入cboss数据，只导入BIP2B147和BIP2B262的
	insert into rpt_cboss_monitor_day(odate, bip_code, trans_id, biz_type, opr_code, user_name, sp_biz_code, 
            user_status, process_time, opr_time, efft_time, rsp_desc, orig_domain, home_prov,deal_time)
	select b.odate,b.bip_code,b.trans_id,b.biz_type,b.opr_code,b.user_name,b.sp_biz_code,b.user_status,b.process_time,b.opr_time,b.efft_time,b.rsp_desc,b.orig_domain,b.home_prov,b.deal_time
	from (
		select to_timestamp(a.arr[1]||' '||a.arr[2],'yyyy/mm/dd hh24:mi:ss') odate,a.bip_code,a.trans_id,a.biz_type,a.opr_code,a.user_name,a.sp_biz_code,a.user_status,
		       a.process_time,a.opr_time,a.efft_time,a.rsp_desc,a.orig_domain,a.home_prov,a.deal_time
		from (
			select string_to_array(part1,' ') arr,
			       trim('} ' from part2) bip_code,
			       trim('} ' from part4) trans_id,
			       trim('} ' from part5) biz_type,
			       trim('} ' from part6) opr_code,
			       trim('} ' from part7) user_name,
			       trim('} ' from part8) sp_biz_code,
			       trim('} ' from part9) user_status,
			       (case when trim('} ' from part11) = '' then null 
			             else to_timestamp(trim('} ' from part11),'yyyymmddhh24miss') end )process_time,
			       (case when trim('} ' from part12) = '' then null 
			             else to_timestamp(trim('} ' from part12),'yyyymmddhh24miss') end )opr_time,
			       (case when trim('} ' from part13) = '' then null 
			             else to_timestamp(trim('} ' from part13),'yyyymmddhh24miss') end )efft_time,
			       trim('} ' from part15) rsp_desc,
			       trim('} ' from part17) orig_domain,
			       (case when trim( '} ' from trim(part20)) ='' then null
			             else trim( '} ' from trim(part20))::integer end ) home_prov,
			       trim('} ' from part21)::numeric deal_time
			from ext_cboss_log
			where trim('} ' from part2) in ( 'BIP2B147','BIP2B262')
		) a
	)b
        where date_trunc('day',odate)=to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_cboss_monitor_day from ext_cboss_do_log start',0);
	
	--从ext_cboss_do_log导入cboss数据，只导入BIP3B022的
	insert into rpt_cboss_monitor_day(odate, bip_code, trans_id, biz_type, opr_code, user_name, sp_biz_code, 
            user_status, process_time, opr_time, efft_time, rsp_desc, orig_domain,deal_time)
	select b.odate,b.bip_code,b.trans_id,b.biz_type,b.opr_code,b.user_name,b.sp_biz_code,b.user_status,b.process_time,b.opr_time,b.efft_time,b.rsp_desc,b.orig_domain,b.deal_time
	from (
		select to_timestamp(a.arr[1]||' '||a.arr[2],'yyyy-mm-dd hh24:mi:ss') odate,a.bip_code,a.trans_id,a.biz_type,a.opr_code,a.user_name,a.sp_biz_code,a.user_status,
		       a.process_time,a.opr_time,a.efft_time,a.rsp_desc,a.orig_domain,a.deal_time
		from (
			select string_to_array(part1,' ') arr,
			       trim('} ' from part2) bip_code,
			       trim('} ' from part4) trans_id,
			       trim('} ' from part5) biz_type,
			       trim('} ' from part6) opr_code,
			       trim('} ' from part7) user_name,
			       trim('} ' from part8) sp_biz_code,
			       trim('} ' from part9) user_status,			       
			       (case when trim('} ' from part11) = '' then null 
			             else to_timestamp(trim('} ' from part11),'yyyymmddhh24miss') end )process_time,
			       (case when trim('} ' from part12) = '' then null 
			             else to_timestamp(trim('} ' from part12),'yyyymmddhh24miss') end )opr_time,
			       (case when trim('} ' from part13) = '' then null 
			             else to_timestamp(trim('} ' from part13),'yyyymmdd hh24miss') end )efft_time,
			       trim('} ' from part15) rsp_desc,
			       'BOSS' orig_domain,
			       trim('}' from part20)::numeric deal_time	      
			from ext_cboss_do_log 
			where trim('} ' from part2) = 'BIP3B022'
		) a
	)b
        where date_trunc('day',odate)=to_date(vi_dealdate,'yyyy-mm-dd');
        
        perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;
   
   