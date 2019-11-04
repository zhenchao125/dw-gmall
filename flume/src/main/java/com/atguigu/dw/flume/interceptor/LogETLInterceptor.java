package com.atguigu.dw.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2019/10/16 2:42 PM
 */
public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 没有 batchSize 参数调用这个方法  可以修改 event, 如果返回 null 表示抛弃这个 event
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 0. 如果 event 是 null
        if (event == null) {
            return null;
        }

        // 1. 获取到日志
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("utf-8"));

        // 2. 过滤日志, 并向 header 中赋值. 不同的过滤日志, 并向 header 中赋值.
        // 不同的日志, 不同的过滤方法日志, 不同的过滤方法
        if (log.contains("\"en\":\"start\"")) {  // 2.1 启动日志
            if (LogUtil.validateStart(log)) {
                return event;
            }
        } else {  // 2.2 事件日志
            if (LogUtil.validateEvent(log)) {
                return event;
            }
        }

        return null;
    }

    /**
     * 有 batchSize 参数则调用这个方法
     * 如果要过滤掉某个 event, 可以把这个 event 从 List 中删除
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> it = events.iterator();
        if (it.hasNext()) {  // 从集合中删除数据的时候, 不应该使用 for 循环, 而应该使用迭代器的删除方法
            Event event = it.next();
            if (intercept(event) == null) {  // 验证之后如果是 null 则从集合中移除数据
                it.remove();
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();  // 直接返回过滤器对象就可以了
        }

        /**
         * 配置文件
         * @param context
         */
        @Override
        public void configure(Context context) {

        }
    }
}
