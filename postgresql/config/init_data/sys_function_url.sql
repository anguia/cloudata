/*系统功能菜单*/



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
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (440, 411, '/report/thumbnail/queryThumbnail01.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (401, 421, '/report/authentication/pagepushsuccess/pagepushsuccess/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (427, 421, '/report/thumbnail/thumbnail02.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (441, 421, '/report/thumbnail/queryThumbnail02.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (402, 422, '/report/authentication/pagepushsuccess/potentialsubscriberanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (403, 431, '/report/authentication/userreq/userreq/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (428, 431, '/report/thumbnail/thumbnail03.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (442, 431, '/report/thumbnail/queryThumbnail03.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (404, 441, '/report/authentication/scanattack/scanattack/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (429, 441, '/report/thumbnail/thumbnail04.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (443, 441, '/report/thumbnail/queryThumbnail04.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (405, 442, '/report/authentication/scanattack/scanattackerror/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (406, 451, '/report/authentication/portalMsg/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (431, 451, '/report/thumbnail/thumbnail06.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (445, 451, '/report/thumbnail/queryThumbnail06.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (407, 452, '/report/authentication/portalMsg/PwdErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (408, 453, '/report/authentication/portalMsg/SubhomePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (409, 453, '/report/authentication/portalMsg/NumTypeAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (410, 453, '/report/authentication/portalMsg/SubErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (411, 454, '/report/authentication/portalMsg/StatusErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (412, 461, '/report/authentication/cha/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (432, 461, '/report/thumbnail/thumbnail07.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (446, 461, '/report/thumbnail/queryThumbnail07.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (413, 462, '/report/authentication/cha/AccountAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (414, 463, '/report/authentication/cha/ACIPErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (415, 464, '/report/authentication/cha/CookieAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (416, 471, '/report/authentication/auth/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (433, 471, '/report/thumbnail/thumbnail08.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (447, 471, '/report/thumbnail/queryThumbnail08.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (417, 481, '/report/authentication/radius/ErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (434, 481, '/report/thumbnail/thumbnail09.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (448, 481, '/report/thumbnail/queryThumbnail09.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (418, 482, '/report/authentication/radius/DNSErrhomePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (419, 482, '/report/authentication/radius/DNSErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (420, 482, '/report/authentication/radius/DNSErrProv/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (421, 483, '/report/authentication/radius/EAPErrhomePage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (422, 483, '/report/authentication/radius/EAPErrAnalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (423, 483, '/report/authentication/radius/EAPErrProv/acIPPage.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (424, 483, '/report/authentication/radius/EAPErrProv/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (425, 500, '/report/authentication/normaluserreq/normaluserreq/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (430, 500, '/report/thumbnail/thumbnail05.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (444, 500, '/report/thumbnail/queryThumbnail05.do**', 'Y');

/*业务分析*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (500, 511, '/report/analyze/order/useranalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (517, 511, '/report/thumbnail/thumbnail10.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (540, 511, '/report/thumbnail/queryThumbnail10.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (518, 511, '/report/thumbnail/thumbnail11.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (541, 511, '/report/thumbnail/queryThumbnail11.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (501, 521, '/report/analyze/activeuser/activeuser/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (519, 521, '/report/thumbnail/thumbnail12.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (542, 521, '/report/thumbnail/queryThumbnail12.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (502, 522, '/report/analyze/activeuser/newuser/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (503, 531, '/report/analyze/bussinessanalyse/allbussanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (504, 541, '/report/analyze/roam/roamanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (520, 541, '/report/thumbnail/thumbnail13.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (543, 541, '/report/thumbnail/queryThumbnail13.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (505, 551, '/report/analyze/timeSubject/timeAnalyze/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (506, 551, '/report/analyze/timeSubject/timeSubjectQuery.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (507, 551, '/report/analyze/timeSubject/timeSubjectExport.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (521, 551, '/report/thumbnail/thumbnail14.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (544, 551, '/report/thumbnail/queryThumbnail14.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (508, 552, '/report/analyze/timeSubject/timebracketanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (525, 552, '/report/thumbnail/thumbnail18.do/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (548, 552, '/report/thumbnail/queryThumbnail18.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (526, 552, '/report/thumbnail/thumbnail19.do/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (549, 552, '/report/thumbnail/queryThumbnail19.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (509, 561, '/report/analyze/octetsSubject/octetsAnalyze/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (510, 561, '/report/analyze/octetsSubject/octetsSubjectQuery.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (511, 561, '/report/analyze/octetsSubject/octetsSubjectExport.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (522, 561, '/report/thumbnail/thumbnail15.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (545, 561, '/report/thumbnail/queryThumbnail15.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (512, 562, '/report/analyze/octetsSubject/flowbracketanalyse/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (524, 562, '/report/thumbnail/thumbnail17.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (547, 562, '/report/thumbnail/queryThumbnail17.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (513, 571, '/report/analyze/numSubject/numAnalyze/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (514, 571, '/report/analyze/numSubject/numSubjectQuery.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (515, 571, '/report/analyze/numSubject/numSubjectExport.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (523, 571, '/report/thumbnail/thumbnail16.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (546, 571, '/report/thumbnail/queryThumbnail16.do**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (516, 581, '/report/analyze/hotspot/hostpotanalyse/**', 'Y');

/*日志认证*/
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (600, 611, '/report/analyze/Log/PortalLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (601, 621, '/report/analyze/Log/RadiusLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (602, 631, '/report/analyze/Log/UsageLog/**', 'Y');
insert into sys_function_url(URL_ID, FUNCTION_ID, URL, VISIBLE) values (603, 641, '/report/analyze/Log/CbossLog/**', 'Y');
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