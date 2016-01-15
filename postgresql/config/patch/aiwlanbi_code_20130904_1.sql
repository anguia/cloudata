CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_active_ua_type_month';
begin
	perform write_runlog(v_func_name,'function start',0);
		
	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm'); 

	perform write_runlog(v_func_name,'insert temp_active_ua_type_month start',0);
	
	--保存部分话单数据到临时表，减少查询压力
	truncate table temp_active_ua_type_month;
	insert into temp_active_ua_type_month(start_time,node_id, user_name, user_domain)
        select t.start_time,t.node_id,t.user_name, t.user_domain
        from src_usage t
        where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm');

        perform write_runlog(v_func_name,'delete rpt_active_ua_type_month start',0);
        
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
			from temp_active_ua_type_month t
			
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
			from temp_active_ua_type_month t
			
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