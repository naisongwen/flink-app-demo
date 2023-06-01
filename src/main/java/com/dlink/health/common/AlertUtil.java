package com.dlink.health.common;

import com.squareup.okhttp.*;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.apache.flink.util.Preconditions;
import org.eclipse.jetty.util.ajax.JSON;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class AlertUtil {
    public static void sendWX(String corpid, String corpsecret, String users, String msg) throws IOException {
        String tokenUrl = String.format("http://wx.cxmt.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s", corpid, corpsecret);
        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request
                .Builder()
                .url(tokenUrl)
                .build();

        Response response = null;
        response = okHttpClient.newCall(request).execute();
        //获取Http Status Code.其中200表示成功
        if (response.code() == 200) {
            //这里需要注意，response.body().string()是获取返回的结果，此句话只能调用一次，再次调用获得不到结果。
            //所以先将结果使用result变量接收
            Map obj = (Map) JSON.parse(response.body().string());
            String token= (String) obj.get("access_token");
            Preconditions.checkNotNull(token,"access_token not found");
            String notifyUrl = String.format("http://wx.cxmt.com/cgi-bin/message/send?access_token=%s", token);
            JSONObject jsonObjectRequest = new JSONObject();
            jsonObjectRequest.put("msgtype", "text");
            JSONObject content = new JSONObject();
            content.put("content",msg);
            jsonObjectRequest.put("text", content);
            jsonObjectRequest.put("touser", users);
            jsonObjectRequest.put("agentid", 1000025);
            jsonObjectRequest.put("safe", 0);
            MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
            RequestBody requestBody = RequestBody.create(mediaType, jsonObjectRequest.toJSONString());
            request = new Request.Builder()
                    .url(notifyUrl)
                    .post(requestBody)
                    .build();
            response=okHttpClient.newCall(request).execute();
            if (response.code() == 200) {
                obj = (Map) JSON.parse(response.body().string());
                log.info("msg sended with jobid:"+obj.get("jobid"));
            }
        }
    }
}
