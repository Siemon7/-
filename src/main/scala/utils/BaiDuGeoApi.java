package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.avro.TestAnnotation;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.junit.Test;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

//java版计算signature签名
public class BaiDuGeoApi {

    public  static String  getBusiness( String latAndLon) throws Exception,
            NoSuchAlgorithmException {

// 计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，
// 该方法根据key的插入顺序排序；post请使用TreeMap保存<key,value>，
// 该方法会自动将key按照字母a-z顺序排序。所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，
// 但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
// 以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，paramsMap中先放入address，
// 再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
        Config load = ConfigFactory.load();
        //ak=您的ak&output=json&coordtype=wgs84ll&location=31.225696563611,121.49884033194

        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("ak", load.getString("yourak"));
        paramsMap.put("output", "json");
        paramsMap.put("coordtype", "wgs84ll");
        paramsMap.put("location", latAndLon);

        //参数
        // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
        String paramsStr = toQueryString(paramsMap);

        //计算sn签名
        String wholeStr = new String("/reverse_geocoding/v3/?" + paramsStr + load.getString("yoursk") );
        // 对上面wholeStr再作utf8编码
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");
        // 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
        String snCal = MD5(tempStr); //sn签名

        String business = null;

        //相当于浏览器
        HttpClient httpClient = new HttpClient();


        GetMethod getMethod = new GetMethod("http://api.map.baidu.com/reverse_geocoding/v3/?" + paramsStr + "&sn=" + snCal);

        int code = httpClient.executeMethod(getMethod);
        if (code == 200){
           //获取本次请求的响应内容
            String responseBody = getMethod.getResponseBodyAsString();

            getMethod.releaseConnection();

                //解析json--fastjson
                JSONObject jsonObject = JSON.parseObject(responseBody);
                JSONObject result = jsonObject.getJSONObject("result");
                business = result.getString("business");
                if(StringUtils.isEmpty(business)) {
                    JSONArray jsonArray = jsonObject.getJSONArray("pois");
                    if(null !=jsonArray && jsonArray.size()>0){
                        String tag = jsonArray.getJSONObject(0).getString("Tag");
                    }
                    }

        }
        return  business;
    }


    // 对Map内所有value作utf8编码，拼接返回结果
    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    public  static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }
}