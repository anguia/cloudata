-- modify function_url with function_id in (551, 561, 571)

delete from sys_function_url where function_id=551;
delete from sys_function_url where function_id=561;
delete from sys_function_url where function_id=571;

INSERT INTO sys_function_url VALUES (506, 551, '/report/analyze/timeSubject/timeSubjectQuery.do**', 'Y');
INSERT INTO sys_function_url VALUES (521, 551, '/report/thumbnail/thumbnail14.do**', 'Y');
INSERT INTO sys_function_url VALUES (507, 551, '/report/analyze/timeSubject/timeSubjectExport.do**', 'Y');
INSERT INTO sys_function_url VALUES (505, 551, '/report/analyze/timeSubject/timeAnalyze/**', 'Y');
INSERT INTO sys_function_url VALUES (511, 561, '/report/analyze/octetsSubject/octetsSubjectExport.do**', 'Y');
INSERT INTO sys_function_url VALUES (509, 561, '/report/analyze/octetsSubject/octetsAnalyze/**', 'Y');
INSERT INTO sys_function_url VALUES (522, 561, '/report/thumbnail/thumbnail15.do**', 'Y');
INSERT INTO sys_function_url VALUES (510, 561, '/report/analyze/octetsSubject/octetsSubjectQuery.do**', 'Y');
INSERT INTO sys_function_url VALUES (513, 571, '/report/analyze/numSubject/numAnalyze/**', 'Y');
INSERT INTO sys_function_url VALUES (523, 571, '/report/thumbnail/thumbnail16.do**', 'Y');
INSERT INTO sys_function_url VALUES (515, 571, '/report/analyze/numSubject/numSubjectExport.do**', 'Y');
INSERT INTO sys_function_url VALUES (514, 571, '/report/analyze/numSubject/numSubjectQuery.do**', 'Y');
