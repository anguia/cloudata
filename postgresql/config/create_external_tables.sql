CREATE EXTERNAL TABLE ext_apache_log (
    part1 text,
    part2 text
) LOCATION (
    'gpfdist://10.3.3.138:8001/apache*.log'
) FORMAT 'text' (delimiter E']' null E'@' escape E'@')
ENCODING 'GBK'
LOG ERRORS INTO public.err_ext_apache_log SEGMENT REJECT LIMIT 1000000000 ROWS;

CREATE EXTERNAL TABLE ext_cboss_do_log
(
  part1 text,
  part2 text,
  part3 text,
  part4 text,
  part5 text,
  part6 text,
  part7 text,
  part8 text,
  part9 text,
  part10 text,
  part11 text,
  part12 text,
  part13 text,
  part14 text,
  part15 text,
  part16 text,
  part17 text,
  part18 text,
  part19 text,
  part20 text
)
 LOCATION (
    'gpfdist://10.3.3.138:8004/domonitor*'
)
 FORMAT 'text' (delimiter '{' null E'\\N' escape E'\\' fill missing fields)
ENCODING 'GBK'
LOG ERRORS INTO err_ext_cboss_do_log SEGMENT REJECT LIMIT 200000000 ROWS;


CREATE EXTERNAL TABLE ext_cboss_log (
    part1 text,
    part2 text,
    part3 text,
    part4 text,
    part5 text,
    part6 text,
    part7 text,
    part8 text,
    part9 text,
    part10 text,
    part11 text,
    part12 text,
    part13 text,
    part14 text,
    part15 text,
    part16 text,
    part17 text,
    part18 text,
    part19 text,
    part20 text,
    part21 text
) LOCATION (
    'gpfdist://10.3.3.138:8004/cboss*'
) FORMAT 'text' (delimiter E'{' null E'\\N' escape E'\\' fill missing fields)
ENCODING 'GBK'
LOG ERRORS INTO public.err_ext_cboss_log SEGMENT REJECT LIMIT 200000000 ROWS;


CREATE EXTERNAL TABLE ext_monitor_log (
    p_date text,
    p_time text,
    pid text,
    op_type text,
    err_type text,
    user_name text,
    user_ip text,
    ac_ip text,
    stype text,
    mon_type text,
    err_detail text,
    cost_time text,
    login_mode text,
    user_agent text,
    is_challenge text,
    user_type text,
    ssid text,
    area_code text
) LOCATION (
    'gpfdist://10.3.3.138:8002/monitor*.log'
) FORMAT 'text' (delimiter E' ' null E'--' escape E'\\')
ENCODING 'GBK'
LOG ERRORS INTO public.err_ext_monitor_log SEGMENT REJECT LIMIT 200000000 ROWS;



CREATE EXTERNAL TABLE ext_radius_log (
    part1 text,
    part2 text,
    part3 text
) LOCATION (
    'gpfdist://10.3.3.138:8003/radius*.log'
) FORMAT 'text' (delimiter E'[' null E' ' escape E'\\')
ENCODING 'GBK'
LOG ERRORS INTO public.err_ext_radius_log SEGMENT REJECT LIMIT 200000000 ROWS;



CREATE EXTERNAL TABLE ext_subscription (
    bms_subscription_id text,
    bms_subscription_begin_time text,
    bms_subscription_end_time text,
    bms_customer_account_cycle text,
    bms_svc_id text,
    bms_user_name text,
    bms_user_password text,
    bms_customer_id text,
    bms_product_id text,
    bms_plan_id text,
    bms_plan_subs_id text,
    abs_price_id text,
    bms_svcauth_id text,
    bms_subscription_status text,
    bms_subscription_status_time text,
    bms_status_remark text,
    bms_svc_type text,
    bms_create_time text,
    bms_creator text,
    bms_creator_group text,
    bms_subscription_no text,
    bms_node_id text,
    bms_region_id text,
    bms_svc_psflag text,
    bms_customer_type text,
    abs_billing_type text,
    abs_rate_type text,
    abs_billing_sttime text,
    bms_user_credit_level text,
    bms_cardtype_id text,
    bms_currency_type text,
    bms_discount_id text,
    bms_customer_modify_time text
) LOCATION (
    'gpfdist://10.3.3.138:8004/bms_subscription_bppp*.txt'
) FORMAT 'text' (delimiter E'|' null E' ' escape E'\\')
ENCODING 'UTF8'
LOG ERRORS INTO public.err_ext_subscription SEGMENT REJECT LIMIT 200000000 ROWS;



CREATE EXTERNAL TABLE ext_usage (
    idr_id text,
    svc_id text,
    start_time text,
    stop_time text,
    idr_status text,
    charge text,
    charge_disc text,
    res_usage_1 text,
    res_usage_2 text,
    res_usage_3 text,
    res_usage_4 text,
    res_type text,
    item_type text,
    time_stamp text,
    node_id text,
    customer_id text,
    customer_type text,
    subscription_id text,
    user_name text,
    bms_svc_id text,
    bms_product_id text,
    abs_price_id text,
    bms_node_id text,
    caller_id text,
    callee_id text,
    nas_ip text,
    idr_type text,
    session_time text,
    input_octets text,
    output_octets text,
    input_packets text,
    output_packets text,
    isdn_channel text,
    bms_customer_pay_type text,
    nas_port text,
    user_domain text,
    roam_domain text,
    session_id text,
    port_type text,
    frame_ip text,
    abs_billing_type text,
    abs_rate_type text,
    bms_cardtype_id text,
    bms_plan_id text,
    bms_currency_type text,
    real_session_time text,
    real_input_octets text,
    real_output_octets text,
    author_type text,
    acct_status text,
    user_type text,
    current_bandwidth text,
    usage_backup text,
    nas_identifier text,
    otherarea_accessid text,
    authen_type text,
    nat_pubip text,
    nat_startport text,
    nat_endport text
) LOCATION (
    'gpfdist://10.3.3.138:8004/usage_bppp_*.txt'
) FORMAT 'text' (delimiter E'|' null E' ' escape E'\\')
ENCODING 'UTF8'
LOG ERRORS INTO public.err_ext_usage SEGMENT REJECT LIMIT 200000000 ROWS;



CREATE EXTERNAL TABLE ext_wlan_package (
    bms_user_name text,
    package_code text,
    package_name text,
    abs_effect_time text,
    abs_expire_time text,
    bms_prefix_type text,
    abs_res_open text,
    time_stamp text
) LOCATION (
    'gpfdist://10.3.3.138:8004/wlan_package*.txt'
) FORMAT 'text' (delimiter E'|' null E' ' escape E'\\')
ENCODING 'GBK'
LOG ERRORS INTO public.err_ext_wlan_package SEGMENT REJECT LIMIT 200000000 ROWS;



CREATE EXTERNAL TABLE ext_wlan_user_cookie (
    bms_user_name text,
    abs_effect_time text,
    abs_expire_time text,
    abs_effect_days text,
    time_stamp text,
    ua_type text
) LOCATION (
    'gpfdist://10.3.3.138:8004/wlan_user_cookie*.txt'
) FORMAT 'text' (delimiter E'|' null E' ' escape E'\\')
ENCODING 'UTF8'
LOG ERRORS INTO public.err_ext_wlan_user_cookie SEGMENT REJECT LIMIT 200000000 ROWS;


