package com.atguigu.dw.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2019/10/16 2:42 PM
 */
public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        if (event == null) return null;

        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("utf8"));
        Map<String, String> headers = event.getHeaders();
        if (log.contains("start")) {
            headers.put("topic", "topic_start");
        } else {
            headers.put("topic", "topic_event");

        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();  // 直接返回过滤器对象就可以了
        }

        /**
         * 配置文件
         *
         * @param context
         */
        @Override
        public void configure(Context context) {

        }
    }
}
