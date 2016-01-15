CREATE OR REPLACE FUNCTION etl_rpt_active_ua_type_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	
begin

	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm'); 

	--清理当前统计日期下的数据
	delete from rpt_active_ua_type_month where to_date(odate,'yyyy-mm') = to_date(vi_dealdate,'yyyy-mm');

	--统计终端类型活跃用户数
	insert into rpt_active_ua_type_month(odate,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm'),
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,1,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent
	) n
	group by to_date(vi_dealDate,'yyyy-mm'),ua_type ;

	--统计省份、终端类型活跃用户数
	insert into rpt_active_ua_type_month(odate,prov_id,ua_type,stat_type,active_user)
	select to_date(vi_dealDate,'yyyy-mm'),n.node_id,
	       (case when position('PC' in upper(n.user_agent)) > 0 then 2 
	             when upper(n.user_agent) = 'UA0047'  then 3
		     when upper(n.user_agent) = 'UA0999' or n.user_agent is null then 4
	             else 1 end  ) ua_type,2,count(n.user_name)
	from (
		--根据用户名分区，时间戳排序的排序号和用户名做关联查询
		select b.user_name,b.user_agent,a.node_id
		from (
			select row_number() over(partition by t.user_name order by t.start_time asc) id,  
			       t.user_name,t.node_id
			from src_usage t
			where t.start_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.start_time < to_timestamp(v_end_day,'yyyy-mm')
		) a,

		(
			select row_number() over(partition by t.user_name order by t.date_time asc) id,
			       t.user_name,t.user_agent
			from temp_login_request_success t
			where t.date_time >= to_timestamp(vi_dealDate,'yyyy-mm')
			and t.date_time < to_timestamp(v_end_day,'yyyy-mm')
		) b
		where a.user_name = b.user_name and a.id = b.id 
		group by b.user_name,b.user_agent,a.node_id
	) n
	group by to_date(vi_dealDate,'yyyy-mm'),n.node_id,ua_type ;
	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_active_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_day text; --结束日期 格式：yyyy-mm
	
begin

	v_end_day = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_active_user_month where to_date(odate,'yyyy-mm')= to_date(vi_dealdate,'yyyy-mm');

	--统计集团活跃用户数
	insert into rpt_active_user_month(odate,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),1,count(a.user_name)
	from(
		select t.user_name	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name
	) a;

	--统计使用地活跃用户数
	insert into rpt_active_user_month(odate,node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,2,count(a.user_name)
	from(
		select t.user_name,t.node_id	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id
	) a
	group by a.node_id;

	--统计归属地活跃用户数
	insert into rpt_active_user_month(odate,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.bms_node_id,3,count(a.user_name)
	from(
		select t.user_name,t.bms_node_id	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.bms_node_id
	) a
	group by a.bms_node_id;

	--统计使用地、acip维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,acip,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.nas_ip,4,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.nas_ip	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,t.nas_ip
	) a
	group by a.node_id,a.nas_ip;

	--统计用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.user_type,5,count(a.user_name)
	from(
		select t.user_name,
		       (case when t.customer_type = 2 then 4
	                     when position('EDU.' in upper(t.user_name)) > 0 and t.customer_type = 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 and t.customer_type = 0 then 3
	                     else 1 end) user_type	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,user_type
	) a
	group by a.user_type;

	--统计使用地、用户类型维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,user_type,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.user_type,6,count(a.user_name)
	from(
		select t.user_name,t.node_id,
		       (case when customer_type = 2 then 4
	                     when position('EDU.' in upper(t.user_name)) > 0 and t.customer_type = 0 then 2
	                     when position('STARBUCKS' in upper(t.user_name)) > 0 and t.customer_type = 0 then 3
	                     else 1 end) user_type	       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,user_type
	) a
	group by a.node_id,a.user_type;

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
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,login_type
	) a
	group by a.login_type;

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
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,login_type
	) a
	group by a.node_id,a.login_type;

	--统计使用地、归属地维度的活跃用户数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,stat_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,a.bms_node_id,9,count(a.user_name)
	from(
		select t.user_name,t.node_id,t.bms_node_id       		
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
		and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
		group by t.user_name,t.node_id,t.bms_node_id
	) a
	group by a.node_id,a.bms_node_id;

	--统计使用地、归属地、acip、用户类型、登录方式维度的wlan时长、wlan流量、wlan使用次数
	insert into rpt_active_user_month(odate,node_id,bms_node_id,acip,user_type,login_type,stat_type,wlan_time,in_out_octets,use_num)
	select to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,
	       (case when customer_type = 2 then 4
	             when position('EDU.' in upper(t.user_name)) > 0 and t.customer_type = 0 then 2
		     when position('STARBUCKS' in upper(t.user_name)) > 0 and t.customer_type = 0 then 3
		     else 1 end) user_type,
	       (case when position('WEB' in upper(t.user_domain)) > 0 then 1
		             when position('CTL' in upper(t.user_domain)) > 0 then 3
		             when t.authen_type = 2 then 2
		             when t.authen_type = 5 then 5 else 1 end ) login_type,99,COALESCE(sum(t.wlan_time),0) wlan_time,
	        COALESCE(sum(t.input_octets + t.output_octets),0) in_out_octets,count(t.user_name) use_num
	from src_usage t
	where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm')
	and t.time_stamp < to_timestamp(v_end_day ,'yyyy-mm')
	group by to_date(vi_dealdate,'yyyy-mm'),t.node_id,t.bms_node_id,t.nas_ip,user_type,login_type;
	
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_hotspot_usage(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
begin
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm')  ;

	--清理当前统计日期下的数据
	delete from rpt_hotspot_usage where to_date(odate,'yyyy-mm') = to_date(vi_dealdate,'yyyy-mm');

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
				     else 1 end) user_type,
			       t.nas_identifier
			from src_usage t
			where t.time_stamp >= to_timestamp(vi_dealdate , 'yyyy-mm-dd')
			and t.time_stamp < to_timestamp( v_end_date , 'yyyy-mm-dd')
			and t.customer_type = 0
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
				     else 1 end) user_type,
			       t.nas_ip
			from src_usage t
			where t.time_stamp >= to_timestamp(vi_dealdate , 'yyyy-mm-dd')
			and t.time_stamp < to_timestamp(v_end_date , 'yyyy-mm-dd')
			and t.customer_type = 0
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
			where t.time_stamp >= to_timestamp(vi_dealdate , 'yyyy-mm-dd')
			and t.time_stamp < to_timestamp(v_end_date , 'yyyy-mm-dd')
			and t.customer_type = 0
			group by t.node_id,
				 t.user_name,
				 t.nas_identifier
		) a
		group by a.node_id,a.nas_identifier
	) b
	group by b.node_id
	) e on e.node_id = d.node_id ) n;
	
	
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_new_active_user_month(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	v_last_month text; --上一个月日期 格式：yyyy-mm
	
begin
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');
	v_last_month = to_char(to_date(vi_dealDate, 'yyyy-mm') - interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_new_active_user_month where to_date(odate,'yyyy-mm')=to_date(vi_dealdate,'yyyy-mm');

	--统计新增活跃用户数
	insert into rpt_new_active_user_month (odate,prov_id,user_type,active_user)
	select to_date(vi_dealdate,'yyyy-mm'),a.node_id,
	       (case when customer_type = 2 then 4
	             when position('EDU.' in upper(a.user_name)) > 0 and a.customer_type = 0 then 2
	             when position('STARBUCKS' in upper(a.user_name)) > 0 and a.customer_type = 0 then 3
	             else 1 end),count(a.user_name)
	from (
		select t.node_id,
		       t.user_name,
		       t.customer_type	       
		from src_usage t
		where t.time_stamp >= to_timestamp(vi_dealdate ,'yyyy-mm-dd')
		and t.time_stamp < to_timestamp(v_end_date ,'yyyy-mm-dd')
		
		group by t.node_id,t.user_name,t.customer_type
	) a
	where not exists(
		select 1 from (
			select t.node_id,
			       t.user_name	       
			from src_usage t
			where t.time_stamp >= to_timestamp(v_last_month ,'yyyy-mm-dd')
			and t.time_stamp < to_timestamp(vi_dealDate ,'yyyy-mm-dd')
			
			group by t.node_id,t.user_name
		) b 
		where b.node_id = a.node_id and b.user_name = a.user_name
	)
	group by to_date(vi_dealdate,'yyyy-mm'),a.node_id,
	       (case when customer_type = 2 then 4
	             when position('EDU.' in upper(a.user_name)) > 0 and a.customer_type = 0 then 2
	             when position('STARBUCKS' in upper(a.user_name)) > 0 and a.customer_type = 0 then 3
	             else 1 end);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION etl_rpt_subscription_day(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	
begin

	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm');

	--清理当前统计日期下的数据
	delete from rpt_subscription_day where odate = to_date(vi_dealdate,'yyyy-mm-dd');

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

	
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;