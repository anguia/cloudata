/*给三个用户赋角色*/
delete from sys_user_role;
insert into sys_user_role (USER_ID, ROLE_ID) values (1, 1);
insert into sys_user_role (USER_ID, ROLE_ID) values (2, 2);
insert into sys_user_role (USER_ID, ROLE_ID) values (3, 3);