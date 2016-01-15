CREATE OR REPLACE FUNCTION etl_rpt_hotspot_usage(vi_dealdate text)
  RETURNS void AS
$BODY$

declare
	v_end_date text; --结束日期 格式：yyyy-mm
	v_func_name text:='etl_rpt_hotspot_usage';
begin
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_hotspot_usage start',0);
	
	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm') + interval '1 month', 'yyyy-mm')  ;

	--清理当前统计日期下的数据
	delete from rpt_hotspot_usage where to_date(odate,'yyyy-mm') = to_date(vi_dealdate,'yyyy-mm');

	perform write_runlog(v_func_name,'insert rpt_hotspot_usage start',0);
	
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
				     when t.user_name ~ '[0-9]{11}' then 1 
				     else 4 end) user_type,
			       t.nas_identifier
			from src_usage t
			where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')			
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
				     when t.user_name ~ '[0-9]{11}' then 1 
				     else 4 end) user_type,
			       t.nas_ip
			from src_usage t
			where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
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
			where date_trunc('month', t.time_stamp) = to_date(vi_dealDate,'yyyy-mm')
			group by t.node_id,
				 t.user_name,
				 t.nas_identifier
		) a
		group by a.node_id,a.nas_identifier
	) b
	group by b.node_id
	) e on e.node_id = d.node_id ) n;
	
	perform write_runlog(v_func_name,'function end',0);
end;

$BODY$
  LANGUAGE plpgsql VOLATILE;