CREATE TABLE sys_run_log
(
  func_name character varying(100),
  log_desc character varying(1000),
  status integer,
  create_time timestamp without time zone
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (func_name);

CREATE OR REPLACE FUNCTION etl_rpt_pwd_err_day(vi_dealdate text)
  RETURNS void AS
$BODY$
begin
	delete from rpt_pwd_err_day where odate = to_date(vi_dealdate, 'yyyy-mm-dd');

	--根据省份，统计部分成功数和完全失败的数
	--此处“其他错误”已经包括“其他错误(OBS)”+“其他错误（PORTAL）”+“其他错误（AC）”
	insert into rpt_pwd_err_day(odate, prov_id, part_failed_num, all_failed_num)
	select to_date(vi_dealdate, 'yyyy-mm-dd') as odate,c.prov_id,
		sum(case when all_num<>fail_num then 1 else 0 end) as part_failed_num,
		sum(case when all_num=fail_num then 1 else 0 end) as all_failed_num
	 from (select ac_ip,count(*) as all_num,
		sum(case when detail_info in ('AC名称不匹配','OBS访问失败','用户没有订购业务 ','用户密码错误','用户状态错误',
			'自动认证已过期(cookie)','动态密码有效期过期','用户上线且使用同一用户名和IP重复登录','用户先上线,然后用另一名字在同一客户机器再认证',
			'认证请求被拒绝','用户没有可用时长','用户卡无效','读取OBS响应包超时','接收AC/BAS响应包超时','其他错误','请求auth，上线BAS错误',
			'请求Challenge，上线BAS错误','请求Challenge被拒绝','请求Challenge此链接已建立','请求Challenge有一个用户正在认证过程中，请稍后再试',
			'认证请求被拒绝(星巴克）') then 1 else 0 end) as fail_num
	from temp_normal_login_request group by ac_ip) t,SYS_prov_ipseg_info c  
	where t.ac_ip between c.start_ip and c.end_ip group by c.prov_id;

end;
$BODY$
  LANGUAGE plpgsql VOLATILE;



