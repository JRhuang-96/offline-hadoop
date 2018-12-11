# offline-hadoop
Hadoop离线计算 :模拟 使用hadoop MR 进行数据清洗,再使用shell 脚本执行hive 进行数据统计,维度分析

## 完整项目架构:

#### 1.收集数据: &nbsp;使用flume 收集 web logs 到 HDFS上;           
#### 2.清洗数据: &nbsp;使用 Hadoop MR 清洗数据;
#### 3.处理数据: &nbsp;使用 HQL 分析数据, 求出日活、日活维度分析、日新、日新维度分析等;  
#### 4.导出数据: &nbsp;使用sqoop将数据导出到mysql 中;
#### 5.编写shell 脚本: &nbsp; 设置每天启动处理数据,重复以上流程.

-------
<h3>本项目只实现</h3> 
<h4>清洗数据-->处理数据-->shell脚本启动处理数据</h4>
