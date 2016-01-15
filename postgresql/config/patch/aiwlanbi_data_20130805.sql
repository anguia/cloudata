﻿truncate table sys_function;

INSERT INTO "sys_function" VALUES (1, 'SYSTEM_BASE', '系统基本功能权限', 1, 0, 0, NULL, NULL, NULL, NULL);
INSERT INTO "sys_function" VALUES (2, 'SYSTEM_MANAGE_INDEX', '主窗口', 0, 0, 0, 'home/frameContent.action', NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (3, 'GDATA_MANAGE_INDEX', '局数据管理', 0, 0, 0, 'setting/provIpsegInfo/dimensionTableHome.do', NULL, NULL, 2);
INSERT INTO "sys_function" VALUES (4, 'REPORT_CERTIFICATE_INDEX', '认证流程报表', 0, 0, 0, 'home/certification.action', NULL, NULL, 3);
INSERT INTO "sys_function" VALUES (5, 'REPORT_ANALYSIS_INDEX', '业务数据报表', 0, 0, 0, 'home/business.action', NULL, NULL, 4);
INSERT INTO "sys_function" VALUES (6, 'REPORT_LOG_INDEX', '日志查询', 0, 0, 0, 'home/log.action', NULL, NULL, 5);
INSERT INTO "sys_function" VALUES (7, 'SYSTEM_MANAGE_INDEX', '系统管理', 0, 0, 0, 'home/configuration.action', NULL, NULL, 6);
INSERT INTO "sys_function" VALUES (41, 'CERTIFICATE_USER_PORTAL', '用户Portal请求', 0, 4, 0, NULL, NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (42, 'CERTIFICATE_PORTAL_PUSH', 'Portal推送页面', 0, 4, 0, NULL, NULL, NULL, 2);
INSERT INTO "sys_function" VALUES (43, 'userLonginReq', '用户上线请求', 0, 4, 0, NULL, NULL, NULL, 3);
INSERT INTO "sys_function" VALUES (44, 'scanattack', '扫号攻击分析', 0, 4, 0, NULL, NULL, NULL, 4);
INSERT INTO "sys_function" VALUES (45, 'portalMsg', 'Portal用户信息验证', 0, 4, 0, NULL, NULL, NULL, 6);
INSERT INTO "sys_function" VALUES (46, 'cha', 'CHALLENGE请求', 0, 4, 0, NULL, NULL, NULL, 7);
INSERT INTO "sys_function" VALUES (47, 'auth', 'AUTH鉴权请求', 0, 4, 0, NULL, NULL, NULL, 8);
INSERT INTO "sys_function" VALUES (48, 'radius', 'Radius认证', 0, 4, 0, NULL, NULL, NULL, 9);
INSERT INTO "sys_function" VALUES (50, 'normaluserreq', '用户正常上线请求分析', 0, 4, 0, NULL, NULL, NULL, 5);
INSERT INTO "sys_function" VALUES (51, 'useranalyse', '订购用户分析', 0, 5, 0, NULL, NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (52, 'activeuser', '活跃用户分析', 0, 5, 0, NULL, NULL, NULL, 2);
INSERT INTO "sys_function" VALUES (53, 'useranalyse', '整体业务量分析', 0, 5, 0, NULL, NULL, NULL, 3);
INSERT INTO "sys_function" VALUES (54, 'roamanalyse', '漫游分析', 0, 5, 0, NULL, NULL, NULL, 4);
INSERT INTO "sys_function" VALUES (55, 'activeuser', '时长专题', 0, 5, 0, NULL, NULL, NULL, 5);
INSERT INTO "sys_function" VALUES (56, 'octetsAnalyze', '流量专题', 0, 5, 0, NULL, NULL, NULL, 6);
INSERT INTO "sys_function" VALUES (57, 'numAnalyze', '次数专题', 0, 5, 0, NULL, NULL, NULL, 7);
INSERT INTO "sys_function" VALUES (58, 'hostpotanalyse', '热点使用分析', 0, 5, 0, NULL, NULL, NULL, 8);
INSERT INTO "sys_function" VALUES (61, 'PortalLog', 'portal认证', 0, 6, 0, NULL, NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (62, 'RadiusLog', 'radius认证', 0, 6, 0, NULL, NULL, NULL, 2);
INSERT INTO "sys_function" VALUES (63, 'UsageLog', '话单详情', 0, 6, 0, NULL, NULL, NULL, 3);
INSERT INTO "sys_function" VALUES (64, 'CbossLog', 'cboss日志', 0, 6, 0, NULL, NULL, NULL, 4);
INSERT INTO "sys_function" VALUES (65, 'SelfServiceLog', '自服务日志', 0, 6, 0, NULL, NULL, NULL, 5);
INSERT INTO "sys_function" VALUES (71, 'USER_MANAGE_INDEX', '用户管理', 0, 7, 0, 'system/user/index.do', NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (411, 'CERTIFICATE_USER_PORTAL_INDEX', 'portal首页请求情况', 0, 41, 0, 'report/authentication/portalpagereq/portalpagereq/portalHomePageRequestAnalysePage.do', NULL, 'ico_authen_protalError', 1);
INSERT INTO "sys_function" VALUES (421, 'CERTIFICATE_PORTAL_PUSH_ANALYSIS', 'portal推送成功分析', 0, 42, 0, 'report/authentication/pagepushsuccess/pagepushsuccess/portalPushSuccessAnalysePage.do', NULL, 'ico_authen_protalPush', 2);
INSERT INTO "sys_function" VALUES (422, 'CERTIFICATE_PORTAL_PUSH_ANALYSIS', '潜在用户分析', 0, 42, 0, 'report/authentication/pagepushsuccess/potentialsubscriberanalyse/potentialSubscriberAnalyseMainPage.do', NULL, 'ico_authen_potentialUser', 3);
INSERT INTO "sys_function" VALUES (431, 'userLonginReqMainPage', '用户上线请求分析', 0, 43, 0, 'report/authentication/userreq/userreq/userLonginReqMainPage.do', NULL, 'ico_authen_userLine', 1);
INSERT INTO "sys_function" VALUES (441, 'scanAttackAnalysePage', '用户扫号攻击行为分析', 0, 44, 0, 'report/authentication/scanattack/scanattack/scanAttackAnalysePage.do', NULL, 'ico_authen_DNSError', 1);
INSERT INTO "sys_function" VALUES (442, 'errorBringedByScanAttackAnalysePage', '用户扫号攻击行为造成的错误原因分析', 0, 44, 0, 'report/authentication/scanattack/scanattackerror/errorBringedByScanAttackAnalysePage.do', NULL, 'ico_authen_userStateError', 2);
INSERT INTO "sys_function" VALUES (451, 'ErrAnalyse', '信息验证环节错误原因', 0, 45, 0, 'report/authentication/portalMsg/ErrAnalyse/page.do', NULL, 'ico_authen_radiusError', 1);
INSERT INTO "sys_function" VALUES (452, 'PwdErrAnalyse', '用户密码错误分析', 0, 45, 0, 'report/authentication/portalMsg/PwdErrAnalyse/page.do', NULL, 'ico_authen_challengeError', 2);
INSERT INTO "sys_function" VALUES (453, 'SubhomePage', '用户未订购业务分析', 0, 45, 0, 'report/authentication/portalMsg/SubhomePage.do', NULL, 'ico_system_ETL', 3);
INSERT INTO "sys_function" VALUES (454, 'StatusErrAnalyse', '用户状态不正确分析', 0, 45, 0, 'report/authentication/portalMsg/StatusErrAnalyse/page.do', NULL, 'ico_authen_top20ACIP', 4);
INSERT INTO "sys_function" VALUES (461, 'ErrAnalyse', 'challenge验证环节错误原因', 0, 46, 0, 'report/authentication/cha/ErrAnalyse/page.do', NULL, 'ico_authen_noOrder', 1);
INSERT INTO "sys_function" VALUES (462, 'AccountAnalyse', '三种错误账号已在线分析', 0, 46, 0, 'report/authentication/cha/AccountAnalyse/page.do', NULL, 'ico_authen_pwError', 2);
INSERT INTO "sys_function" VALUES (463, 'ACIPErrAnalyse', '三种challenge错误top20 ACIP', 0, 46, 0, 'report/authentication/cha/ACIPErrAnalyse/page.do', NULL, 'ico_authen_sweepingAttackError', 3);
INSERT INTO "sys_function" VALUES (464, 'CookieAnalyse', 'cookie认证分析', 0, 46, 0, 'report/authentication/cha/CookieAnalyse/page.do', NULL, 'ico_authen_challengeCookie', 4);
INSERT INTO "sys_function" VALUES (471, 'ErrAnalyse', 'AUTH鉴权环节问题分析', 0, 47, 0, 'report/authentication/auth/ErrAnalyse/page.do', NULL, 'ico_authen_sweepingAttack', 1);
INSERT INTO "sys_function" VALUES (481, 'ErrAnalyse', 'Radius请求环节问题分析', 0, 48, 0, 'report/authentication/radius/ErrAnalyse/page.do', NULL, 'ico_authen_originalLine', 1);
INSERT INTO "sys_function" VALUES (482, 'DNSErrhomePage', '域名错误详细分析', 0, 48, 0, 'report/authentication/radius/DNSErrhomePage.do', NULL, 'ico_data_provIP', 2);
INSERT INTO "sys_function" VALUES (483, 'EAPErrhomePage', 'EAP消息超时详细分析', 0, 48, 0, 'report/authentication/radius/EAPErrhomePage.do', NULL, 'ico_authen_EAPError', 3);
INSERT INTO "sys_function" VALUES (500, 'userNormalLonginReqMainPage', '用户正常上线请求分析', 0, 50, 0, 'report/authentication/normaluserreq/normaluserreq/userNormalLonginReqMainPage.do', NULL, 'ico_data_ACIP', 1);
INSERT INTO "sys_function" VALUES (511, 'UserOrderMain', '用户订购情况', 0, 51, 0, 'report/analyze/order/useranalyse/UserOrderMain.do', NULL, 'ico_business_newUserReport', 1);
INSERT INTO "sys_function" VALUES (521, 'activeUserMainPage', '活跃用户', 0, 52, 0, 'report/analyze/activeuser/activeuser/activeUserMainPage.do', NULL, 'ico_business_activeUser', 2);
INSERT INTO "sys_function" VALUES (522, 'newActiveUserPage', '新增活跃用户', 0, 52, 0, 'report/analyze/activeuser/newuser/newActiveUserPage.do', NULL, 'ico_business_allBussine', 3);
INSERT INTO "sys_function" VALUES (531, 'wholeOperationMainPage', '整体业务', 0, 53, 0, 'report/analyze/bussinessanalyse/allbussanalyse/wholeOperationMainPage.do', NULL, 'ico_business_flow', 1);
INSERT INTO "sys_function" VALUES (541, 'flowAnalysisPage', '漫游分析', 0, 54, 0, 'report/analyze/roam/roamanalyse/flowAnalysisPage.do', NULL, 'ico_business_order', 1);
INSERT INTO "sys_function" VALUES (551, 'timeAnalyze', '使用时长分析', 0, 55, 0, 'report/analyze/timeSubject/timeAnalyze/timeAnalyze.do', NULL, 'ico_business_timeReport', 1);
INSERT INTO "sys_function" VALUES (552, 'stepMainPage', '用户使用时长分档', 0, 55, 0, 'report/analyze/timeSubject/timebracketanalyse/stepMainPage.do', NULL, 'ico_business_time', 2);
INSERT INTO "sys_function" VALUES (561, 'octetsAnalyze', '使用流量分析', 0, 56, 0, 'report/analyze/octetsSubject/octetsAnalyze/octetsAnalyze.do', NULL, 'ico_business_flowReport', 1);
INSERT INTO "sys_function" VALUES (562, 'stepFlowPage', '用户使用流量分档', 0, 56, 0, 'report/analyze/octetsSubject/flowbracketanalyse/stepFlowPage.do', NULL, 'ico_business_roaming', 2);
INSERT INTO "sys_function" VALUES (571, 'flowAnalysisPage', '使用次数分析', 0, 57, 0, 'report/analyze/numSubject/numAnalyze/numAnalyze.do', NULL, 'ico_business_account', 1);
INSERT INTO "sys_function" VALUES (581, 'HotSpotUsageMain', '热点使用情况', 0, 58, 0, 'report/analyze/hotspot/hostpotanalyse/HotSpotUsageMain.do', NULL, 'ico_business_hotPoint', 1);
INSERT INTO "sys_function" VALUES (611, 'PortalLog', 'portal认证日志查询', 0, 61, 0, 'report/analyze/Log/PortalLog/PortalLog.do', NULL, 'ico_log_protal', 1);
INSERT INTO "sys_function" VALUES (621, 'RadiusLog', 'radius认证日志查询', 0, 62, 0, 'report/analyze/Log/RadiusLog/RadiusLog.do', NULL, 'ico_log_radius', 1);
INSERT INTO "sys_function" VALUES (631, 'UsageLog', '话单详细信息查询', 0, 63, 0, 'report/analyze/Log/UsageLog/UsageLog.do', NULL, 'ico_log_phoneFile', 1);
INSERT INTO "sys_function" VALUES (641, 'CbossLog', 'cboss日志查询', 0, 64, 0, 'report/analyze/Log/CbossLog/CbossLog.do', NULL, 'ico_log_cboss', 1);
INSERT INTO "sys_function" VALUES (651, 'SelfServiceLog', '自服务日志查询', 0, 65, 0, 'report/analyze/Log/SelfServiceLog/SelfServiceLog.do', NULL, 'ico_log_self', 1);
INSERT INTO "sys_function" VALUES (711, 'USER_MANAGE_INDEX', '用户管理', 0, 71, 0, 'system/user/index.do', NULL, 'ico_system_user', 1);
INSERT INTO "sys_function" VALUES (712, 'ROLE_MANAGE_INDEX', '角色管理', 0, 71, 0, 'system/role/index.do', NULL, 'ico_system_role', 2);
INSERT INTO "sys_function" VALUES (713, 'ACTION_LOG_INDEX', '操作日志', 0, 71, 0, 'system/log/index.do', NULL, 'ico_system_operate', 3);
INSERT INTO "sys_function" VALUES (714, 'ETL_RUN_LOG_INDEX', 'ETL运行日志', 0, 71, 0, 'system/etl/index.do', NULL, 'ico_system_ETL', 4);
INSERT INTO "sys_function" VALUES (7111, 'CREATE_USER', '创建用户', 0, 711, 0, NULL, NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (7112, 'REMOVE_USER', '删除用户', 0, 711, 0, NULL, NULL, NULL, 2);
INSERT INTO "sys_function" VALUES (7113, 'MODIFY_USER', '修改用户信息', 0, 711, 0, NULL, NULL, NULL, 3);
INSERT INTO "sys_function" VALUES (7114, 'RESET_PWD', '重置密码', 0, 711, 0, NULL, NULL, NULL, 4);
INSERT INTO "sys_function" VALUES (7115, 'MODIFY_DATA_PRIV', '修改用户数据权限', 0, 711, 0, NULL, NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (7121, 'CREATE_ROLE', '创建角色', 0, 712, 0, NULL, NULL, NULL, 1);
INSERT INTO "sys_function" VALUES (7122, 'REMOVE_ROLE', '删除角色', 0, 712, 0, NULL, NULL, NULL, 2);
INSERT INTO "sys_function" VALUES (7123, 'MODIFY_ROLE', '修改角色', 0, 712, 0, NULL, NULL, NULL, 3);
INSERT INTO "sys_function" VALUES (7124, 'MODIFY_ROLE_AUTHORITY', '修改角色授权', 0, 712, 0, NULL, NULL, NULL, 4);

