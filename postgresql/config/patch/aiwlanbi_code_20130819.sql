﻿CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_month(vi_dealdate text)
  RETURNS void AS
$BODY$
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
			where date_trunc('month', t.start_time) = to_date(vi_dealDate,'yyyy-mm')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where date_trunc('month', t.date_time) = to_date(vi_dealDate,'yyyy-mm')
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
			where date_trunc('month', t.start_time) = to_date(vi_dealDate,'yyyy-mm')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where date_trunc('month', t.date_time) = to_date(vi_dealDate,'yyyy-mm')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent,a.node_id,a.user_domain
	) n
	group by to_date(vi_dealDate,'yyyy-mm'),n.node_id,ua_type ;

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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
		group by t.user_name
	) a;

	perform write_runlog(v_func_name,'insert rpt_active_user_month(stat_type=2) start',0);
	
	--统计使用地活跃用户数
	insert into rpt_active_user_month(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from src_usage t
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
		where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
	where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
	group by to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,user_type,login_type;

	perform write_runlog(v_func_name,'function end',0);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;