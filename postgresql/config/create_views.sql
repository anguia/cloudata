CREATE VIEW all_sys_prov_acip_info AS
    SELECT temp_sys_prov_acip_info.idx, temp_sys_prov_acip_info.ac_prov_id, temp_sys_prov_acip_info.ac_city_name, temp_sys_prov_acip_info.ac_name, temp_sys_prov_acip_info.ac_ip, temp_sys_prov_acip_info.create_time, temp_sys_prov_acip_info.update_time FROM temp_sys_prov_acip_info UNION ALL SELECT sys_prov_acip_info.idx, sys_prov_acip_info.ac_prov_id, sys_prov_acip_info.ac_city_name, sys_prov_acip_info.ac_name, sys_prov_acip_info.ac_ip, sys_prov_acip_info.create_time, sys_prov_acip_info.update_time FROM sys_prov_acip_info;



CREATE VIEW all_sys_prov_ipseg_info AS
    SELECT temp_sys_prov_ipseg_info.idx, temp_sys_prov_ipseg_info.prov_id, temp_sys_prov_ipseg_info.subnet_mask, temp_sys_prov_ipseg_info.start_ip, temp_sys_prov_ipseg_info.end_ip, temp_sys_prov_ipseg_info.create_time, temp_sys_prov_ipseg_info.update_time FROM temp_sys_prov_ipseg_info UNION ALL SELECT sys_prov_ipseg_info.idx, sys_prov_ipseg_info.prov_id, sys_prov_ipseg_info.subnet_mask, sys_prov_ipseg_info.start_ip, sys_prov_ipseg_info.end_ip, sys_prov_ipseg_info.create_time, sys_prov_ipseg_info.update_time FROM sys_prov_ipseg_info;


