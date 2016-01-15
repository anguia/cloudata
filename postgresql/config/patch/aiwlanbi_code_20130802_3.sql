CREATE OR REPLACE FUNCTION etl_rpt_apache_month(vi_dealdate text)
  RETURNS void AS
$BODY$

begin

	--------统计Portal访问相关信息, 月报表统计
	delete from rpt_apache_month where odate = to_date(vi_dealdate, 'yyyy-mm'); 
	insert into rpt_apache_month(odate, prov_id, SUCCESS_NUM, PV_NUM, UV_NUM, POTENTIAL_USER_NUM, INTRO_PAGE_NUM)
	select a.m_odate, a.prov_id, COALESCE(SUCCESS_NUM,0), COALESCE(PV_NUM,0), COALESCE(UV_NUM,0), COALESCE(POTENTIAL_USER_NUM,0), COALESCE(INTRO_PAGE_NUM, 0)
	from (
		select date_trunc('month', odate) :: date as m_odate, prov_id, sum(SUCCESS_NUM) as SUCCESS_NUM
			,  sum(PV_NUM) as PV_NUM
		from RPT_APACHE_DAY
		where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm')
		group by m_odate, prov_id
	) a
	left join (
		select m_odate, prov_id, sum(case when u_count = u_sum then 1 else 0 end ) as POTENTIAL_USER_NUM
			, sum(intro_page_num ) as INTRO_PAGE_NUM
			, sum(UV_NUM) as UV_NUM
		from (
			select date_trunc('month', odate) :: date as m_odate, prov_id, user_ip
				, max(potential_user)  as POTENTIAL_USER_NUM
				, max(intro_page) as INTRO_PAGE_NUM
				, max(uv_flag)  as UV_NUM
				, count(1) as u_count, sum(potential_user) as u_sum
			from SRC_APACHE_MONTH_LOG
			where date_trunc('month', odate) :: date = to_date(vi_dealdate, 'yyyy-mm')
			group by m_odate, prov_id, user_ip
		) a
		group by m_odate, prov_id
	) b on a.m_odate = b.m_odate and a.prov_id = b.prov_id;	
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;

delete from sys_user_data_privilege;
insert into sys_user_data_privilege (user_id, prov_code)
select 1,  prov_id from sys_prov_info;