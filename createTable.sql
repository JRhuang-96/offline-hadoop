--强制删除数据库
--drop database etl_tb cascade;


CREATE database if not exists etl_tb;
use etl_tb;

--创建外部分区表,映射MR清洗后的数据
CREATE external TABLE etl_cleared_info(
imei				      string,
sdk_ver  			    string,
time_zone			    string,
commit_id			    string,
commit_time		    string,
pid					      string,
app_token  		    string,
app_id	 			    string,
device_id			    string,
device_id_type	  string,
release_channel		string,
app_ver_name 	 	  string,
app_ver_code		  string,
os_name				    string,
os_ver  			    string,
language			    string,
country				    string,
manufacture			  string,
device_model		  string,
resolution			  string,
net_type			    string,
account				    string,
app_device_id 		string,
mac					      string,
android_id			  string,
user_id 			    string,
cid_sn  			    string,
build_num			    string,
mobile_data_type 	string,
promotion_channel string,
carrier 			    string,
city 				      string
)
partitioned by (day string)
row format delimited 
fields terminated by ' '
lines terminated by '\n'
location '/app_cleared_data/'
;


--日活表 -每日活动用户 (create DAU table)
CREATE TABLE etl_day_active_user_info (
imei 				      string,
sdk_ver 			    string,
time_zone 			  string,
commit_id 			  string,
commit_time 		  string,
pid 			    	  string,
app_token 			  string,
app_id 				    string,
device_id 			  string,
device_id_type 	  string,
release_channel   string,
app_ver_name		  string,
app_ver_code 		  string,
os_name 			    string,
os_ver 				    string,
language 			    string,
country 			    string,
manufacture 		  string,
device_model 		  string,
resolution 			  string,
net_type 			    string,
account 			    string,
app_device_id 	  string,
mac 				      string,
android_id 			  string,
user_id 			    string,
cid_sn 				    string,
build_num 			  string,
mobile_data_type  string,
promotion_channel string,
carrier 			    string,
city 				      string
) 
partitioned BY (day string) 
row format delimited 
fields terminated BY ' '
lines terminated BY '\n';


--日新表 -每日新增用户
CREATE TABLE etl_day_new_user_info  like etl_day_active_user_info ;

--历史用户表 
CREATE TABLE etl_user_history_info(user_id string)  ;

--日活用户维度表
CREATE TABLE dim_day_user_active_info(
sdk_ver 		    string,
app_ver_name 	  string,
app_ver_code	  string,
os_name			    string,
city			      string,
manufacture		  string,
nums			      int
) 
partitioned by (day string, dim string)
row format delimited
fields terminated by ' '
lines terminated by '\n'
;

--日新用户维度表
CREATE TABLE dim_day_new_user_info(
sdk_ver 		    string,
app_ver_name 	  string,
app_ver_code	  string,
os_name			    string,
city			      string,
manufacture		  string,
nums			      int
)
partitioned by (day string, dim string)
row format delimited
fields terminated by ' '
lines terminated by '\n'
;

--CREATE TABLE dim_day_new_user_info like dim_day_user_active_info;


--次日留存表
CREATE table retain_oneday_ago_info(
imei				      string,
sdk_ver  			    string,
time_zone			    string,
commit_id			    string,
commit_time			  string,
pid					      string,
app_token  			  string,
app_id	 			    string,
device_id			    string,
device_id_type	  string,
release_channel	  string,
app_ver_name 	 	  string,
app_ver_code		  string,
os_name				    string,
os_ver  			    string,
language			    string,
country				    string,
manufacture			  string,
device_model		  string,
resolution			  string,
net_type			    string,
account				    string,
app_device_id 	  string,
mac					      string,
android_id			  string,
user_id 			    string,
cid_sn  			    string,
build_num			    string,
mobile_data_type 	string,
promotion_channel string,
carrier 			    string,
city 				      string
) 
partitioned by (day string)
;

