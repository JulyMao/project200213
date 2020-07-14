package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author maow
 * @create 2020-06-24 15:29
 */
public class TimeStampInterceptor implements Interceptor {

    private ArrayList<Event> lists = new ArrayList<>();
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String s = new String(event.getBody(), StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(s);
        String ts = jsonObject.getString("ts");
        headers.put("timestamp",ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        lists.clear();
        for (Event event : events){
            lists.add(intercept(event));
        }
        return lists;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
