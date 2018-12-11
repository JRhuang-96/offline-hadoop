#!/bin/bash
#
#get yesterday time
oneday_ago=`date -d yesterday +%Y-%m-%d`
twodays_ago=`date -d "-2 days" +%Y-%m-%d`
eightdays_ago=`date -d "-8 days" +%Y-%m-%d`
thirtyonedays_ago=`date -d"-31 days" +%Y-%m-%d`

set -e
inpath=/app-log-data/data/
outpath=/app-log-data/clean/day=${oneday_ago}
#
hive_exec=/home/hadoop/software/hive/bin/hive
#
add_data_sql="
load data inpath \"${outpath}/part-m*\" into table etl_cleared_info partition(day=\"$oneday_ago\");
"
#
#计算日活  
calculate_day_user_active_sql="
insert into etl_day_active_user_info partition(day=\"$oneday_ago\")
select 
t.imei              ,
t.sdk_ver 			,
t.time_zone  	 	,
t.commit_id  		,
t.commit_time 		,
t.pid 				,
t.app_token  		,
t.app_id 			,
t.device_id 		,
t.device_id_type 	,
t.release_channel 	,
t.app_ver_name		,
t.app_ver_code 		,
t.os_name 			,
t.os_ver 			,
t.language 			,
t.country 			,
t.manufacture 		,
t.device_model 		,
t.resolution 		,
t.net_type 			,
t.account 			,
t.app_device_id 	,
t.mac 				,
t.android_id 		,
t.user_id 			,
t.cid_sn 			,
t.build_num 		,
t.mobile_data_type 	,
t.promotion_channel ,
t.carrier 			,
t.city 				 
from ( 
	select *,
	row_number()over(partition by user_id order by commit_time) as r
	from etl_cleared_info 
	where day = \"$oneday_ago\")t
where r = 1;

"
#
#计算日新
calculate_daily_new_user_sql="
insert into table etl_day_new_user_info partition(day=\"$oneday_ago\")
select 
imei                 ,
sdk_ver 			 ,
time_zone 			 ,
commit_id 			 ,
commit_time 		 ,
pid 				 ,
app_token 			 ,
app_id 				 ,
device_id 			 ,
device_id_type 		 ,
release_channel 	 ,
app_ver_name		 ,
app_ver_code 		 ,
os_name 			 ,
os_ver 				 ,
language 			 ,
country 			 ,
manufacture 		 ,
device_model 		 ,
resolution 			 ,
net_type 			 ,
account 			 ,
app_device_id 		 ,
mac 				 ,
android_id 			 ,
t1.user_id 			 ,
cid_sn 				 ,
build_num 			 ,
mobile_data_type 	 ,
promotion_channel 	 ,
carrier 			 ,
city
from etl_day_active_user_info t1 left join etl_user_history_info  t2 
on t1.user_id = t2.user_id 
where t1.day = \"$oneday_ago\" and t2.user_id is null;

"
#将日新追加到历史用户表中
append_new_user_tohistory_sql="
insert into table etl_user_history_info
select user_id from etl_day_new_user_info where day=\"$oneday_ago\" ;

"
#
#维度计算
#日活维度
dim_day_user_active_sql="
from etl_day_active_user_info 

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='000000')
	select 'all','all','all','all','all','all',count(1) 
	where day = \"$oneday_ago\"

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='100000')
	select sdk_ver,'all','all','all','all','all',count(1)
	where day = \"$oneday_ago\"
	group by sdk_ver

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='010000')
	select 'all',app_ver_name,'all','all','all','all',count(1) 
	where day = \"$oneday_ago\"
	group by app_ver_name

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='001000')
	select 'all','all',app_ver_code,'all','all','all',count(1)
	where day = \"$oneday_ago\"
	group by app_ver_code

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='000100')
	select 'all','all','all',os_name,'all','all',count(1) 
	where day = \"$oneday_ago\"
	group by os_name

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='000010')
	select 'all','all','all','all',city ,'all',count(1)
	where day = \"$oneday_ago\"
	group by city

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='000001')
	select 'all','all','all','all','all',manufacture,count(1)
	where day = \"$oneday_ago\"
	group by manufacture

--根据业务添加其他的维度分析
--可以多重维度分析

insert into dim_day_user_active_info partition(day=\"$oneday_ago\",dim='000101')
	select 'all','all','all',os_name,'all',manufacture,count(1)
	where day = \"$oneday_ago\"
	group by os_name,manufacture


	;

"
#日新维度计算
dim_day_new_user_sql="
from etl_day_new_user_info 

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='000000')
	select 'all','all','all','all','all','all',count(1) 
	where day = \"$oneday_ago\"

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='100000')
	select sdk_ver,'all','all','all','all','all',count(1)
	where day = \"$oneday_ago\"
	group by sdk_ver

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='010000')
	select 'all',app_ver_name,'all','all','all','all',count(1) 
	where day = \"$oneday_ago\"
	group by app_ver_name

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='001000')
	select 'all','all',app_ver_code,'all','all','all',count(1) 
	where day = \"$oneday_ago\"
	group by app_ver_code

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='000100')
	select 'all','all','all',os_name,'all','all',count(1) 
	where day = \"$oneday_ago\"
	group by os_name

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='000010')
	select 'all','all','all','all',city ,'all',count(1)
	where day = \"$oneday_ago\"
	group by city

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='000001')
	select 'all','all','all','all','all',manufacture,count(1)
	where day = \"$oneday_ago\"
	group by manufacture


--根据业务添加其他的维度分析

insert into dim_day_new_user_info partition(day=\"$oneday_ago\",dim='000011')
	select 'all','all','all','all',city,manufacture,count(1)
	where day = \"$oneday_ago\"
	group by city,manufacture

	;

"
#
#留存计算
#次日留存计算
#
retain_oneday_sql="
insert into table retain_oneday_ago_info partition (day=\"$twodays_ago\")
select		
t1.imei					,
t1.sdk_ver  			,
t1.time_zone			,
t1.commit_id			,
t1.commit_time			,
t1.pid					,
t1.app_token  			,
t1.app_id	 			,
t1.device_id			,
t1.device_id_type		,
t1.release_channel		,
t1.app_ver_name 	 	,
t1.app_ver_code		,
t1.os_name				,
t1.os_ver  			,
t1.language			,
t1.country				,
t1.manufacture			,
t1.device_model		,
t1.resolution			,
t1.net_type			,
t1.account				,
t1.app_device_id 		,
t1.mac					,
t1.android_id			,
t1.user_id 			,
t1.cid_sn  			,
t1.build_num			,
t1.mobile_data_type 	,
t1.promotion_channel 	,
t1.carrier 			,
t1.city 				 
 from 
(select * from etl_day_active_user_info where day =\"$twodays_ago\")t1
left join
(select user_id from etl_day_active_user_info where day =\"$oneday_ago\")t2 
on t1.user_id = t2.user_id
where  t2.user_id is not null
;

"
#.....
#.....
#.....
#执行命令
#
echo "开始执行脚本..."
hadoop jar /home/hadoop/script/app_cleandata.jar outline_calcul.AppDataClean $inpath  $outpath   
#hadoop MR clean the data --hadoop 执行 清洗数据的jar
echo '清洗数据完成 ,进行数据计算'
#
hive -e  "
use etl_tb;  
-- append data into table etl_cleared_info 将数据导入数据表
$add_data_sql;

--calculate  DAU(daily active user)  计算日活
$calculate_day_user_active_sql

--calculate the dimension of DAU   计算日活的维度
$dim_day_user_active_sql

--calculate DNU(daily new user)  计算日新
$calculate_daily_new_user_sql

--append   将日新追加到历史用户表中
$append_new_user_tohistory_sql

--calculate the dimension of DNU  计算日新的维度
$dim_day_new_user_sql

--calculate the D1U   计算次日留存
$retain_oneday_sql



" || { echo "command failed"; exit 1; }
echo"脚本执行完成...."