/*系统默认用户详细信息*/
delete from sys_user_info;
insert into sys_user_info (user_id, user_name, ORG_ID, EMAIL, MOBILE, PHONE, ADDRESS, CREATE_TIME, UPDATE_TIME, DESCRIPTION, EDITABLE) values(1,'系统管理员',0,NULL,NULL,NULL,NULL,now(), now(),NULL,'N');
insert into sys_user_info (user_id, user_name, ORG_ID, EMAIL, MOBILE, PHONE, ADDRESS, CREATE_TIME, UPDATE_TIME, DESCRIPTION, EDITABLE) values(2,'集团管理员',0,NULL,NULL,NULL,NULL,now(), now(),NULL,'N');
insert into sys_user_info (user_id, user_name, ORG_ID, EMAIL, MOBILE, PHONE, ADDRESS, CREATE_TIME, UPDATE_TIME, DESCRIPTION, EDITABLE) values(3,'省管理员',20,NULL,NULL,NULL,NULL,now(), now(),NULL,'N');