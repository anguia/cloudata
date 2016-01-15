-- Function: etl_rpt_wlan_auth_day(text)

-- DROP FUNCTION etl_rpt_wlan_auth_day(text);

CREATE OR REPLACE FUNCTION etl_rpt_wlan_auth_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_func_name text:='etl_rpt_wlan_auth_day';
begin
	----------WLAN认证阶段分析
	perform write_runlog(v_func_name,'delete RPT_WLAN_AUTH_DAY start',0);
	delete from RPT_WLAN_AUTH_DAY where odate  = to_date(vi_dealdate, 'yyyy-mm-dd');

	perform write_runlog(v_func_name,'insert RPT_WLAN_AUTH_DAY start',0);
	insert into RPT_WLAN_AUTH_DAY(odate, prov_id, PORTAL_INDEX_REQUEST, AC_PUSH_PORTAL, ALL_LOGIN_REQUEST, NORMAL_LOGIN_REQUEST
		,CHA_REQUEST, AUTH_REQUEST, RADIUS_REQUEST, AUTH_SUCCESS)
	SELECT a.odate, a.prov_id, COALESCE(PORTAL_INDEX_REQUEST, 0), COALESCE(AC_PUSH_PORTAL, 0)
		, COALESCE(ALL_LOGIN_REQUEST, 0), COALESCE(NORMAL_LOGIN_REQUEST, 0)
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL, 0) as CHA_REQUEST
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL - ERR_NUM, 0) as AUTH_REQUEST
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL - ERR_NUM - auth_ERR_NUM, 0) as RADIUS_REQUEST
		, COALESCE(NORMAL_LOGIN_REQUEST - FAILED_TOTAL - ERR_NUM - auth_ERR_NUM - radius_auth_ERR_NUM, 0) as AUTH_SUCCESS
	FROM (
		-----
		select odate,  prov_ID, COALESCE(PV_NUM , 0)as PORTAL_INDEX_REQUEST
			, COALESCE(SUCCESS_NUM, 0) AC_PUSH_PORTAL
		from RPT_APACHE_DAY
		where odate  = to_date(vi_dealdate, 'yyyy-mm-dd')
	) a
	left join (
		select a.odate, a.prov_id, scan_num + normal_request_day_num as ALL_LOGIN_REQUEST
		from (
			select odate, prov_id, sum( COALESCE(other_errors, 0) + COALESCE(failed_total, 0) + COALESCE(success_total,0)) as scan_num
			from RPT_SCAN_DAY 
			where odate = to_date(vi_dealdate, 'yyyy-mm-dd') 
			group by odate, prov_id
		) a
		left join (
			select odate, prov_id , sum( COALESCE(other_errors, 0) + COALESCE(failed_total, 0) + COALESCE(success_total,0) ) as normal_request_day_num
			from RPT_NORMAL_REQUEST_DAY
			where odate = to_date(vi_dealdate, 'yyyy-mm-dd') 
			group by odate, prov_id
		) b on a.prov_id = b.prov_id
	) b on a.prov_id = b.prov_id
	left join (
		select a.odate, a.prov_id, sum( COALESCE(a.other_errors, 0) + COALESCE(a.failed_total, 0) + COALESCE(a.success_total,0)) as NORMAL_LOGIN_REQUEST
		from RPT_NORMAL_REQUEST_DAY a
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by a.odate, a.prov_id
	) c on a.prov_id = c.prov_id
	left join (
		select odate , prov_id, sum(COALESCE(WRONG_PWD, 0) + COALESCE(NO_SUBSCRIPTION, 0)
			+ COALESCE(WRONG_STATUS, 0) + COALESCE(AUTO_EXPIRED, 0) 
			+ COALESCE(PWD_EXPIRED, 0) + COALESCE(CARD_EXPIRED, 0)
			+ COALESCE(NO_WLAN_TIME, 0) + COALESCE(OBS_FAILED, 0) 
			+ COALESCE(OTHER_OBS_FAILED, 0) + COALESCE(OTHER_PORTAL_FAILED, 0) ) as FAILED_TOTAL 
		from RPT_NORMAL_REQUEST_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) d on a.prov_id = d.prov_id
	left join (
		select odate, prov_id, sum(COALESCE(ERR_NUM, 0)) as ERR_NUM 
		from RPT_CHA_NASIP_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) e on a.prov_id = e.prov_id
	left join (
		select odate, prov_id, COALESCE(CONNECTED_NUM, 0) + COALESCE(blocked_num, 0) +  COALESCE(bas_err_num, 0) as auth_ERR_NUM 
		from RPT_AUTH_REQUEST_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
	) f on a.prov_id = f.prov_id
	left join (
		select odate, prov_id, sum(COALESCE(limit3, 0) + COALESCE(wrong_pwd, 0) + COALESCE(dns_not_found, 0) + COALESCE(eap_timeout, 0)) as radius_auth_ERR_NUM 
		from RPT_RADIUS_AUTH_DAY
		where odate = to_date(vi_dealdate, 'yyyy-mm-dd')
		group by odate, prov_id
	) g on a.prov_id = g.prov_id;

	perform write_runlog(v_func_name,'function end',0);
end

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;