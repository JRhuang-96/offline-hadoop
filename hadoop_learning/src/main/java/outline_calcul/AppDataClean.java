package outline_calcul;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;



public class AppDataClean {
    public static class TokenMapper extends Mapper<Object,Text,Text,NullWritable> {

        Text k = new Text();

        NullWritable val = NullWritable.get();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            JSONObject record = (JSONObject)JSON.parse(value.toString());
            String headerStr = record.getString("header");

            //获取内嵌 header
            JSONObject header = (JSONObject)JSONObject.parse(headerStr);

            //必选字段--清理数据
            if(header.getString("imei")==null||"".equals(header.getString("imei")))return;
            if (header.getString("sdk_ver") ==null||"".equals(header.getString("sdk_ver")))return;
            if (header.getString("time_zone") ==null||"".equals(header.getString("time_zone")))return;
            if (header.getString("commit_id") ==null||"".equals(header.getString("commit_id")))return;
            if (header.getString("commit_time") ==null||"".equals(header.getString("commit_time")))return;
            if (header.getString("pid") ==null||" ".equals(header.getString("pid")))return;
            if (header.getString("app_token") ==null||" ".equals(header.getString("app_token")))return;
            if (header.getString("app_id") ==null||" ".equals(header.getString("app_id")))return;
            if (header.getString("device_id") ==null||" ".equals(header.getString("device_id")))return;
            if (header.getString("device_id_type") ==null||" ".equals(header.getString("device_id_type")))return;
            if (header.getString("release_channel") ==null||" ".equals(header.getString("release_channel")))return;
            if (header.getString("app_ver_name") ==null||" ".equals(header.getString("app_ver_name")))return;
            if (header.getString("app_ver_code") ==null||" ".equals(header.getString("app_ver_code")))return;
            if (header.getString("os_name") ==null||" ".equals(header.getString("os_name")))return;
            if (header.getString("os_ver") ==null||" ".equals(header.getString("os_ver")))return;
            if (header.getString("language") ==null||" ".equals(header.getString("language")))return;
            if (header.getString("country") ==null||" ".equals(header.getString("country")))return;
            if (header.getString("manufacture") ==null||" ".equals(header.getString("manufacture")))return;
            if (header.getString("device_model") ==null||" ".equals(header.getString("device_model")))return;
            if (header.getString("resolution") ==null||" ".equals(header.getString("resolution")))return;
            if (header.getString("net_type") ==null||" ".equals(header.getString("net_type")))return;
            if (header.getString("user_id") ==null||" ".equals(header.getString("user_id")))return;
        
            String res =  StringUtils.getStr(header);

            k.set(res);
            context.write(k,val);

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf =new Configuration();
        Job job = Job.getInstance(conf,"data_clean");

        job.setJarByClass(AppDataClean.class);

        job.setMapperClass(TokenMapper.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置为0 就只有 map 端输出
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }




}
