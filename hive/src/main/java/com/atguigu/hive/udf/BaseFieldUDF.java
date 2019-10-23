package com.atguigu.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 用于解析公共字段, 和把具体事件的 json 字符串解析出来
 *
 * @Author lzc
 * @Date 2019/10/19 5:08 PM
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String key) throws JSONException {

        // 1. 对参数做非空验证
        if (line == null || key == null) return "";

        // 2. 切割每行数据  服务器时间|{....}
        String[] log = line.split("\\|");
        // 3. 对字符串数组做合法验证
        if (log.length != 2 || StringUtils.isBlank(log[1])) return "";

        // 4. 开始解析 json 数据
        JSONObject baseJson = new JSONObject(log[1].trim());
        String resultValue = "";
        switch (key) {
            case "et":   // event 事件
                if (baseJson.has("et")) {
                    resultValue = baseJson.getString("et");
                }
                break;
            case "st":  // server_time
                resultValue = log[0].trim();
                break;
            default:
                JSONObject cm = baseJson.getJSONObject("cm");
                if (cm.has(key)) {
                    resultValue = cm.getString(key);
                }
                break;
        }

        // 3. 返回最终的值
        return resultValue;
    }


    public static void main(String[] args) throws JSONException {
        String json = "1572710416769|{cm\":{\"ln\":\"-96.5\",\"sv\":\"V2.5.4\",\"os\":\"8.1.0\",\"g\":\"WNQ0841X@gmail.com\",\"mid\":\"996\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"19\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"996\",\"t\":\"1572630332748\",\"la\":\"6.1\",\"md\":\"Huawei-2\",\"vn\":\"1.2.3\",\"ba\":\"Huawei\",\"sr\":\"M\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1572690830272\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"9\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"201\",\"loading_way\":\"2\"}},{\"ett\":\"1572661565848\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"5\",\"action\":\"2\",\"detail\":\"\",\"source\":\"1\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"3\"}},{\"ett\":\"1572678449953\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1572626009475\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1572661024827\",\"praise_count\":195,\"other_id\":0,\"comment_id\":5,\"reply_count\":47,\"userid\":9,\"content\":\"蔫象弱身治\"}},{\"ett\":\"1572693068951\",\"en\":\"favorites\",\"kv\":{\"course_id\":4,\"id\":0,\"add_time\":\"1572695909174\",\"userid\":8}}]}\n";
        System.out.println(new BaseFieldUDF().evaluate(json, "st"));
    }
}
/*

12344|"cm": {
        "ln": "-96.5",
        "sv": "V2.5.4",
        "os": "8.1.0",
        "g": "WNQ0841X@gmail.com",
        "mid": "996",
        "nw": "3G",
        "l": "es",
        "vc": "19",
        "hw": "640*960",
        "ar": "MX",
        "uid": "996",
        "t": "1572630332748",
        "la": "6.1",
        "md": "Huawei-2",
        "vn": "1.2.3",
        "ba": "Huawei",
        "sr": "M"
    },
    "ap": "app",
    "et": [
        {
            "ett": "1572690830272",
            "en": "loading",
            "kv": {
                "extend2": "",
                "loading_time": "9",
                "action": "3",
                "extend1": "",
                "type": "1",
                "type1": "201",
                "loading_way": "2"
            }
        },
        {
            "ett": "1572661565848",
            "en": "ad",
            "kv": {
                "entry": "1",
                "show_style": "5",
                "action": "2",
                "detail": "",
                "source": "1",
                "behavior": "1",
                "content": "1",
                "newstype": "3"
            }
        },
        {
            "ett": "1572678449953",
            "en": "active_background",
            "kv": {
                "active_source": "3"
            }
        },
        {
            "ett": "1572626009475",
            "en": "comment",
            "kv": {
                "p_comment_id": 1,
                "addtime": "1572661024827",
                "praise_count": 195,
                "other_id": 0,
                "comment_id": 5,
                "reply_count": 47,
                "userid": 9,
                "content": "蔫象弱身治"
            }
        },
        {
            "ett": "1572693068951",
            "en": "favorites",
            "kv": {
                "course_id": 4,
                "id": 0,
                "add_time": "1572695909174",
                "userid": 8
            }
        }
    ]
}
 */