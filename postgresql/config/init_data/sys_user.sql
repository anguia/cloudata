/*初始化三个系统默认用户*/
delete from sys_user;
insert into sys_user (user_id, user_code, user_pwd, RECENT_PWD, STATUS, TRY_TIMES, PWD_EXPIRED_TIME, LAST_LOGIN_TIME, FAIL_LOGIN_TIMES) values(1,'admin',md5('admin'),'','0','5',now(), now(),'0');
insert into sys_user (user_id, user_code, user_pwd, RECENT_PWD, STATUS, TRY_TIMES, PWD_EXPIRED_TIME, LAST_LOGIN_TIME, FAIL_LOGIN_TIMES) values(2,'gdata',md5('secret'),'','0','5',now(), now(),'0');
insert into sys_user (user_id, user_code, user_pwd, RECENT_PWD, STATUS, TRY_TIMES, PWD_EXPIRED_TIME, LAST_LOGIN_TIME, FAIL_LOGIN_TIMES) values(3,'reporter',md5('secret'),'','0','5',now(), now(),'0');