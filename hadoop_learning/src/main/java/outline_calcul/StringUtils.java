package outline_calcul;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.SimpleDateFormat;

public class StringUtils {

    public static String getStr(JSONObject json){
        StringBuffer buffer = new StringBuffer();
        buffer.append(json.getString("imei")).append(" ").append(json.getString("sdk_ver")).append(" ");
        buffer.append(json.getString("time_zone")).append(" ").append(json.getString("commit_id")).append(" ");
        buffer.append(json.getString("commit_time")).append(" ").append(json.getString("pid")).append(" ");
        buffer.append(json.getString("app_token")).append(" ").append(json.getString("app_id")).append(" ");
        buffer.append(json.getString("device_id")).append(" ").append(json.getString("device_id_type")).append(" ");
        buffer.append(json.getString("release_channel")).append(" ").append(json.getString("app_ver_name")).append(" ");
        buffer.append(json.getString("app_ver_code")).append(" ").append(json.getString("os_name")).append(" ");
        buffer.append(json.getString("os_ver")).append(" ").append(json.getString("language")).append(" ");
        buffer.append(json.getString("country")).append(" ").append(json.getString("manufacture")).append(" ");
        buffer.append(json.getString("device_model")).append(" ").append(json.getString("resolution")).append(" ");
        buffer.append(json.getString("net_type")).append(" ").append(json.getString("account")).append(" ");
        buffer.append(json.getString("app_device_id")).append(" ").append(json.getString("mac")).append(" ");
        buffer.append(json.getString("android_id")).append(" ").append(json.getString("user_id")).append(" ");
        buffer.append(json.getString("cid_sn")).append(" ").append(json.getString("build_num")).append(" ");
        buffer.append(json.getString("mobile_data_type")).append(" ").append(json.getString("promotion_channel")).append(" ");
        buffer.append(json.getString("carrier")).append(" ").append(json.getString("city"));

        return buffer.toString();

    }

    /**
     获取当前的 年月日
     */
    public static String getFileName() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(System.currentTimeMillis());
    }

    public static String getTime(String oldTime){
        FastDateFormat fastDateFormat =  FastDateFormat.getInstance("yyyyMMdd");

        return fastDateFormat.format(oldTime);
    }



}
