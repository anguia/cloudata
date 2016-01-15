CREATE OR REPLACE FUNCTION etl_rpt_active_user_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm-dd
	v_func_name text:='etl_rpt_active_user_day';
begin
	perform write_runlog(v_func_name,'function start',0);
		
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert temp_active_user_day start',0);
	
	--保存部分话单数据到临时表，减少查询压力
	truncate table temp_active_user_day;
	insert into temp_active_user_day(node_id, bms_node_id, user_name, nas_ip, user_type, login_type, 
					 wlan_time, in_out_octets)
        select t.node_id,t.bms_node_id,t.user_name,t.nas_ip,
	       (case when t.user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CLT' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,
	       t.wlan_time,t.input_octets + t.output_octets
        from src_usage t
        where date_trunc('day', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm-dd');

        perform write_runlog(v_func_name,'delete rpt_active_user_day start',0);
	--清理当前统计日期下的数据
	delete from rpt_active_user_day where odate= to_date(vi_dealdate,'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=1) start',0);
	
	--统计集团活跃用户数
	insert into rpt_active_user_day(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from temp_active_user_day t
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_day(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from temp_active_user_day t
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=3) start',0);
	
	--统计归属地活跃用户数
	insert into rpt_active_user_day(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from temp_active_user_day t
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=4) start',0);
	
	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=5) start',0);
	
	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_day(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,t.user_type	       		
		from temp_active_user_day t
		group by t.user_name,t.user_type
	) a
	group by a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=6) start',0);
	
	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.user_type	       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.user_type
	) a
	group by a.node_id,a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=7) start',0);
	
	--统计登录方式维度的活跃用户数
	insert into rpt_active_user_day(odate,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.login_type,7,count(a.user_name)
	from(
		select t.user_name,t.login_type       		
		from temp_active_user_day t
		group by t.user_name,t.login_type
	) a
	group by a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=8) start',0);
	
	--统计使用地、登录方式维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.login_type,8,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.login_type       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.login_type
	) a
	group by a.node_id,a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=9) start',0);
	
	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm-dd'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from temp_active_user_day t
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_day(stat_type=99) start',0);
	
	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_day(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type,99,sum(t.wlan_time),
	       sum(t.in_out_octets) in_out_octets,count(t.user_name)
	from temp_active_user_day t
	group by to_date(vi_dealdate,'yyyy-mm-dd'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type;

	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
CREATE OR REPLACE FUNCTION etl_rpt_active_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_active_user_month';
begin
	perform write_runlog(v_func_name,'function start',0);
		
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	perform write_runlog(v_func_name,'insert temp_active_user_month start',0);
	
	--保存部分话单数据到临时表，减少查询压力
	truncate table temp_active_user_month;
	insert into temp_active_user_month(node_id, bms_node_id, user_name, nas_ip, user_type, login_type, 
					 wlan_time, in_out_octets)
        select t.node_id,t.bms_node_id,t.user_name,t.nas_ip,
	       (case when t.user_name ~ '^[0-9]{11}$' then 1
	                     when position('EDU.' in upper(t.user_name)) > 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 then 3
	                     else 4 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CLT' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,
	       t.wlan_time,t.input_octets + t.output_octets
        from src_usage t
        where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm');

        perform write_runlog(v_func_name,'delete rpt_active_user_month start',0);
	--清理当前统计日期下的数据
	delete from rpt_active_user_month where to_date(odate,'yyyy-mm')= to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=1) start',0);
	
	--统计集团活跃用户数
	insert into rpt_active_user_month(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from temp_active_user_month t
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_month(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from temp_active_user_month t
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=3) start',0);
	
	--统计归属地活跃用户数
	insert into rpt_active_user_month(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from temp_active_user_month t
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=4) start',0);
	
	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=5) start',0);
	
	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,t.user_type	       		
		from temp_active_user_month t
		group by t.user_name,t.user_type
	) a
	group by a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=6) start',0);
	
	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.user_type	       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.user_type
	) a
	group by a.node_id,a.user_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=7) start',0);
	
	--统计登录方式维度的活跃用户数
	insert into rpt_active_user_month(odate,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.login_type,7,count(a.user_name)
	from(
		select t.user_name,t.login_type       		
		from temp_active_user_month t
		group by t.user_name,t.login_type
	) a
	group by a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=8) start',0);
	
	--统计使用地、登录方式维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,login_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.login_type,8,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.login_type       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.login_type
	) a
	group by a.node_id,a.login_type;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=9) start',0);
	
	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from temp_active_user_month t
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=99) start',0);
	
	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type,99,sum(t.wlan_time) wlan_time,
	       sum(t.in_out_octets) in_out_octets,count(t.user_name) use_num
	from temp_active_user_month t
	group by to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,t.user_type,t.login_type;

	perform write_runlog(v_func_name,'function end',0);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;