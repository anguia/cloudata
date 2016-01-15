CREATE OR REPLACE FUNCTION etl_rpt_step_status(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_step_status';
begin
	--上线请求各个阶段成功失败数统计
	--阶段,取值1-7，说明如下：
	--1:portal首页请求
	--2:ac推送portal首页成功
	--3:用户上线申请
	--4:正常上线申请
	--5:challenge请求
	--6:auth请求
	--7:radius请求
	perform write_runlog(v_func_name,'function start',0);
	perform write_runlog(v_func_name,'delete rpt_step_status start',0);
	delete from rpt_step_status where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_step_status1 start',0);
	--1:portal首页请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,1 as step,success_num,pv_num-success_num as failed_num,0 as network_failed_num
	from rpt_apache_day where odate= to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_step_status2 start',0);
	--2:ac推送portal首页成功
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,r.prov_id,2 as step,m.m_num,r.success_num-m.m_num as failed_num,0 as network_failed_num
	from rpt_apache_day r,
		(select p.ac_prov_id as prov_id,count(*) as m_num from src_monitor_log s,sys_prov_acip_info p where s.date_time between to_date(vi_dealdate, 'yyyy-mm-dd')
		 and to_date(vi_dealdate, 'yyyy-mm-dd')+1 and s.ac_ip=p.ac_ip and s.user_ip is not null group by p.ac_prov_id) m
	where r.prov_id=m.prov_id and odate= to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert rpt_step_status3 start',0);
	--3:用户上线申请
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,a.prov_id,3 as step,sum(a.success_total+b.success_total) as success_num,
		sum(a.failed_total+b.failed_total) as failed_num,sum(a.obs_failed+a.OBS_RESP_EXPIRED+a.AC_BAS_RESP_EXPIRED+a.AUTH_BAS_ERR+a.CHA_BAS_ERR
		+b.obs_failed+b.OBS_RESP_EXPIRED+b.AC_BAS_RESP_EXPIRED+b.AUTH_BAS_ERR+b.CHA_BAS_ERR) as network_failed_num
	from rpt_scan_day a,rpt_normal_request_day b 
	where a.odate=b.odate and a.prov_id=b.prov_id and a.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and b.odate=to_date(vi_dealdate, 'yyyy-mm-dd')
	group by a.prov_id;

	perform write_runlog(v_func_name,'insert rpt_step_status4 start',0);
	--4:正常上线申请
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,prov_id,4 as step,sum(success_total) as success_total,sum(failed_total) as failed_num,0 as network_failed_num
	from rpt_normal_request_day where odate=to_date(vi_dealdate, 'yyyy-mm-dd') group by prov_id;

	perform write_runlog(v_func_name,'insert rpt_step_status5 start',0);
	--5:challenge请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,5 as step,n.success_num-(connected+rejected+blocked+no_acip_found
		+bas_err+timeout+acname_not_matched+dup_login) as success_total,(connected+rejected+blocked+no_acip_found
		+bas_err+timeout+acname_not_matched+dup_login) as failed_num,0 as network_failed_num
	from rpt_cha_request_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=4;

	perform write_runlog(v_func_name,'insert rpt_step_status6 start',0);	
	--6:auth请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,6 as step,n.success_num - (connected_num+blocked_num+bas_err_num) as success_total,
		(connected_num+blocked_num+bas_err_num) as failed_num,0 as network_failed_num
	from rpt_auth_request_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=5;

	perform write_runlog(v_func_name,'insert rpt_step_status7 start',0);
	--7:radius请求
	insert into rpt_step_status(odate, prov_id, step, success_num, failed_num, network_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,7 as step,n.success_num - (limit3+wrong_pwd+dns_not_found+eap_timeout) as success_total,
		(limit3+wrong_pwd+dns_not_found+eap_timeout) as failed_num,0 as network_failed_num 
	from rpt_radius_auth_day c,rpt_step_status n
	where c.prov_id=n.prov_id and c.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.odate=to_date(vi_dealdate, 'yyyy-mm-dd') and n.step=6;
	
	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;