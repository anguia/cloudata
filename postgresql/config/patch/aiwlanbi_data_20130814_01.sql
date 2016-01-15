
/*初始化三个系统默认用户*/
delete from sys_user;
insert into sys_user (user_id, user_code, user_pwd, RECENT_PWD, STATUS, TRY_TIMES, PWD_EXPIRED_TIME, LAST_LOGIN_TIME, FAIL_LOGIN_TIMES) values(1,'admin',md5('admin'),'','0','5',now(), now(),'0');
insert into sys_user (user_id, user_code, user_pwd, RECENT_PWD, STATUS, TRY_TIMES, PWD_EXPIRED_TIME, LAST_LOGIN_TIME, FAIL_LOGIN_TIMES) values(2,'gdata',md5('secret'),'','0','5',now(), now(),'0');
insert into sys_user (user_id, user_code, user_pwd, RECENT_PWD, STATUS, TRY_TIMES, PWD_EXPIRED_TIME, LAST_LOGIN_TIME, FAIL_LOGIN_TIMES) values(3,'reporter',md5('secret'),'','0','5',now(), now(),'0');

/*系统默认用户详细信息*/
delete from sys_user_info;
insert into sys_user_info (user_id, user_name, ORG_ID, EMAIL, MOBILE, PHONE, ADDRESS, CREATE_TIME, UPDATE_TIME, DESCRIPTION, EDITABLE) values(1,'系统管理员',0,NULL,NULL,NULL,NULL,now(), now(),NULL,'N');
insert into sys_user_info (user_id, user_name, ORG_ID, EMAIL, MOBILE, PHONE, ADDRESS, CREATE_TIME, UPDATE_TIME, DESCRIPTION, EDITABLE) values(2,'集团管理员',0,NULL,NULL,NULL,NULL,now(), now(),NULL,'N');
insert into sys_user_info (user_id, user_name, ORG_ID, EMAIL, MOBILE, PHONE, ADDRESS, CREATE_TIME, UPDATE_TIME, DESCRIPTION, EDITABLE) values(3,'省管理员',20,NULL,NULL,NULL,NULL,now(), now(),NULL,'N');

/*三种系统角色类型*/
delete from sys_role_type;
insert into sys_role_type(ROLE_TYPE,TYPE_NAME) values(1,'系统管理员');
insert into sys_role_type(ROLE_TYPE,TYPE_NAME) values(2,'集团管理员');
insert into sys_role_type(ROLE_TYPE,TYPE_NAME) values(3,'省管理员');

/*创建三个默认角色*/
delete from sys_role;
insert into sys_role (ROLE_ID, ROLE_NAME, ROLE_STATUS, ROLE_TYPE, CREATE_TIME, UPDATE_TIME, EDITABLE, REMARK) values (1, '系统管理员', 0, 1, now(), null, 'N', '系统管理员');
insert into sys_role (ROLE_ID, ROLE_NAME, ROLE_STATUS, ROLE_TYPE, CREATE_TIME, UPDATE_TIME, EDITABLE, REMARK) values (2, '集团管理员', 0, 2, now(), null, 'N', '集团管理员');
insert into sys_role (ROLE_ID, ROLE_NAME, ROLE_STATUS, ROLE_TYPE, CREATE_TIME, UPDATE_TIME, EDITABLE, REMARK) values (3, '省管理员', 0, 3, now(), null, 'N', '省管理员');

/*给三个用户赋角色*/
delete from sys_user_role;
insert into sys_user_role (USER_ID, ROLE_ID) values (1, 1);
insert into sys_user_role (USER_ID, ROLE_ID) values (2, 2);
insert into sys_user_role (USER_ID, ROLE_ID) values (3, 3);


/*给三个用户赋省份权限*/
delete from sys_user_data_privilege;
insert into sys_user_data_privilege (user_id, prov_code)
select 1 ,prov_id from sys_prov_info;
insert into sys_user_data_privilege (user_id, prov_code)
select 2 ,prov_id from sys_prov_info;
insert into sys_user_data_privilege (user_id, prov_code)
values(3,20);


/*系统操作日志内容明显*/
delete from sys_action_type;

insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (100,11,'用户登录');
--insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (101,11,'注销登录');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (111,11,'增加用户');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (112,11,'删除用户');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (113,11,'修改用户信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (117,11,'修改密码');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (118,11,'重置密码');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (119,11,'修改用户个人信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (131,13,'增加角色');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (132,13,'删除角色');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (133,13,'修改角色');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (134,13,'修改角色授权');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (135,13,'修改数据查看权限');

insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (231,23,'增加省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (232,23,'修改省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (233,23,'删除省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (234,23,'批量导入省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (281,28,'增加ACIP信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (282,28,'修改ACIP信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (283,28,'删除ACIP信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (284,28,'批量导入ACIP信息');

/*系统功能菜单*/
--这一块初始化有凯哥负责



/*system base url*/
delete from sys_function_url;
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (100, 1, '/login.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (101, 1, '/captcha-image.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (102, 1, '/main.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (103, 1, '/loginComplete.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (104, 1, '/userInfo.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (105, 1, '/system/user/getOrgList.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (106, 1, '/system/user/getRoleList.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (107, 1, '/system/user/changePwd.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (108, 1, '/system/user/savePwd.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (109, 1, '/logout.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (110, 1, '/system/user/selfEditPage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (111, 1, '/system/user/editUser.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (112, 1, '/system/user/viewUser.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (113, 1, '/home/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (114, 1, '/widget/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (116, 1, '/frame/**', 'Y');

/*局数据管理*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (300, 3, '/setting/provIpsegInfo/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (301, 3, '/setting/provACIpInfo/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (302, 3, '/importFlie/**', 'Y');

/*认证流程*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (400, 411, '/report/authentication/portalpagereq/portalpagereq/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (426, 411, '/report/thumbnail/thumbnail01.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (401, 421, '/report/authentication/pagepushsuccess/pagepushsuccess/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (427, 421, '/report/thumbnail/thumbnail02.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (402, 422, '/report/authentication/pagepushsuccess/potentialsubscriberanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (403, 431, '/report/authentication/userreq/userreq/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (428, 431, '/report/thumbnail/thumbnail03.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (404, 441, '/report/authentication/scanattack/scanattack/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (429, 441, '/report/thumbnail/thumbnail04.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (405, 442, '/report/authentication/scanattack/scanattackerror/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (406, 451, '/report/authentication/portalMsg/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (431, 451, '/report/thumbnail/thumbnail06.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (407, 452, '/report/authentication/portalMsg/PwdErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (408, 453, '/report/authentication/portalMsg/SubhomePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (409, 453, '/report/authentication/portalMsg/NumTypeAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (410, 453, '/report/authentication/portalMsg/SubErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (411, 454, '/report/authentication/portalMsg/StatusErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (412, 461, '/report/authentication/cha/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (432, 461, '/report/thumbnail/thumbnail07.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (413, 462, '/report/authentication/cha/AccountAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (414, 463, '/report/authentication/cha/ACIPErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (415, 464, '/report/authentication/cha/CookieAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (416, 471, '/report/authentication/auth/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (433, 471, '/report/thumbnail/thumbnail08.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (417, 481, '/report/authentication/radius/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (434, 481, '/report/thumbnail/thumbnail09.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (418, 482, '/report/authentication/radius/DNSErrhomePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (419, 482, '/report/authentication/radius/DNSErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (420, 482, '/report/authentication/radius/DNSErrProv/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (421, 483, '/report/authentication/radius/EAPErrhomePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (422, 483, '/report/authentication/radius/EAPErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (423, 483, '/report/authentication/radius/EAPErrProv/acIPPage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (424, 483, '/report/authentication/radius/EAPErrProv/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (425, 500, '/report/authentication/normaluserreq/normaluserreq/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (430, 500, '/report/thumbnail/thumbnail05.do**', 'Y');

/*业务分析*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (500, 511, '/report/analyze/order/useranalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (517, 511, '/report/thumbnail/thumbnail10.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (518, 511, '/report/thumbnail/thumbnail11.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (501, 521, '/report/analyze/activeuser/activeuser/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (519, 521, '/report/thumbnail/thumbnail12.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (502, 522, '/report/analyze/activeuser/newuser/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (503, 531, '/report/analyze/bussinessanalyse/allbussanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (504, 541, '/report/analyze/roam/roamanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (520, 541, '/report/thumbnail/thumbnail13.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (505, 551, '/report/analyze/timeSubject/timeAnalyze/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (506, 551, '/report/analyze/subject/subjectQuery.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (507, 551, '/report/analyze/subject/subjectExport.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (521, 551, '/report/thumbnail/thumbnail14.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (508, 552, '/report/analyze/timeSubject/timebracketanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (525, 552, '/report/thumbnail/thumbnail18.do/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (526, 552, '/report/thumbnail/thumbnail19.do/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (509, 561, '/report/analyze/octetsSubject/octetsAnalyze/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (510, 561, '/report/analyze/subject/subjectQuery.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (511, 561, '/report/analyze/subject/subjectExport.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (522, 561, '/report/thumbnail/thumbnail15.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (512, 562, '/report/analyze/octetsSubject/flowbracketanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (524, 562, '/report/thumbnail/thumbnail17.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (513, 571, '/report/analyze/numSubject/numAnalyze/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (514, 571, '/report/analyze/subject/subjectQuery.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (515, 571, '/report/analyze/subject/subjectExport.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (523, 571, '/report/thumbnail/thumbnail16.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (516, 581, '/report/analyze/hotspot/hostpotanalyse/**', 'Y');

/*日志认证*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (600, 611, '/report/analyze/Log/PortalLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (601, 621, '/report/analyze/Log/RadiusLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (602, 631, '/report/analyze/Log/UsageLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (603, 641, '/report/analyze/Log/CbossLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (604, 651, '/report/analyze/Log/SelfServiceLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (605, 661, '/report/analyze/Log/AllLog/**', 'Y');

/*用户管理*/
		/*用户管理模块*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (700, 711, '/system/user/index.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (701, 711, '/system/user/list.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (702, 7111, '/system/user/edit.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (703, 7111, '/system/user/addUser.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (704, 7113, '/system/user/edit.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (705, 7113, '/system/user/updateUser.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (706, 7114, '/system/user/resetPwd.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (707, 7114, '/system/user/updatePwd.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (708, 7112, '/system/user/delete.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (709, 7115, '/system/user/dataPrivilegePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (710, 7115, '/system/user/selectedProvList.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (711, 7115, '/system/user/selectableProvList.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (712, 7115, '/system/user/saveDataPrivilege.do**', 'Y');

		/*角色管理模块*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (713, 712, '/system/role/index.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (714, 712, '/system/role/list.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (715, 712, '/system/role/queryRole.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (716, 7121, '/system/role/goRoleEdit.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (717, 7121, '/system/role/saveNew.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (718, 7123, '/system/role/goRoleEdit.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (719, 7123, '/system/role/updateRole.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (720, 7122, '/system/role/delete.do**', 'Y');

insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (721, 7124, '/system/role/updateRoleFun.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (722, 7124, '/system/role/goRoleMenu.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (723, 7124, '/system/role/initMenu.do**', 'Y');

		/*操作日志*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (724, 713, '/system/log/index.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (725, 713, '/system/log/query.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (726, 713, '/system/log/actionType.do**', 'Y');

		/*ETL运行日志*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (727, 714, '/system/etl/index.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (728, 714, '/system/etl/query.do**', 'Y');
--剩下的那些需要先确定上面那一块





/*给三种角色类型初始化不同的菜单权限选择项*/
delete from sys_func_role_type;
insert into sys_func_role_type
select 1, FUNCTION_ID from sys_function;
insert into sys_func_role_type(role_type,function_id)
select 2, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_pid not in(7,71,711,712);
insert into sys_func_role_type(role_type,function_id)
select 3, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_id != 3 and a.function_pid not in(7,71,711,712);




/*给三种角色类型初始化不同的权限*/
delete from sys_func_role;

INSERT INTO sys_func_role(role_id, function_id)
SELECT 1, function_id FROM sys_function ORDER BY function_id;

INSERT INTO sys_func_role(role_id, function_id)
select 2, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_pid not in(7,71,711,712);

INSERT INTO sys_func_role(role_id, function_id)
select 3, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_id != 3 and a.function_pid not in(7,71,711,712);









