CREATE OR REPLACE FUNCTION etl_src_usage(vi_dealdate text)
  RETURNS void AS
$BODY$
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
	where to_date(t.time_stamp,'yyyy-mm-dd') = to_date(vi_dealdate,'yyyy-mm-dd')
	and t.nas_ip is not null and t.node_id is not null and t.bms_node_id is not null;

	perform write_runlog(v_func_name,'function end',0);
end

$BODY$
  LANGUAGE plpgsql VOLATILE;