package com.atguigu.dw.flume.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

/**
 * @Author lzc
 * @Date 2019/10/16 2:55 PM
 */
public class LogUtil {
    /**
     * 验证启动日志
     * {"action":"1","ar":"MX","ba":"HTC","detail":"542","en":"start","entry":"2","extend1":"","g":"S3HQ7LKM@gmail.com","hw":"640*960","l":"en","la":"-43.4","ln":"-98.3","loading_time":"10","md":"HTC-5","mid":"993","nw":"WIFI","open_ad_type":"1","os":"8.2.1","sr":"D","sv":"V2.9.0","t":"1559551922019","uid":"993","vc":"0","vn":"1.1.5"}
     * @param log
     * @return
     */
    public static boolean validateStart(String log) {

        /*if(log == null || log.length() == 0){
            return false;
        }*/
        // 1. 如果是空字符则返回 false
        if (StringUtils.isBlank(log)) return false;
        // 2. 是否合法的 json 字符串对象
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")) return false;

        // 3. 前面都通过则返回 true
        return true;
    }

    /**
     * 验证事件日志
     *
     * 服务器事件|json
     *
     * 1549696569054 | {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}
     * @param log
     * @return
     */
    public static boolean validateEvent(String log) {
        // 1.
        if (StringUtils.isBlank(log)) return false;
        //2. 判断时间戳是否有效
        String[] tsAndJson = log.split("\\|");
        String ts = tsAndJson[0].trim();
        if(ts.length() != 13 || !NumberUtils.isDigits(ts)) return false;
        //3. 判断 json 是否合法
        String logJson = tsAndJson[1].trim();
        if (!logJson.startsWith("{") || !logJson.endsWith("}")) return false;

        // 4. 前面都通过 则返回 true
        return true;
    }
}
