package com.atguigu.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2019/10/19 6:06 PM
 */
public class EventJsonUDTD extends GenericUDTF {
    /*
    1. 对输入参数做验
    2. 返回输出参数的检测器
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if (allStructFieldRefs.size() != 1) {
            throw new UDFArgumentException("参数的个数只能是 1");
        }
        // 输出参数的类型必须是 string
        if (!"string".equals(allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("参数的类型必须是 string");
        }

        // 将来我们要炸两列, 每列的类型.   列名不重要, 因为将来用户会覆盖这个列名
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    // 输入一条记录, 输出若干条结果  一进多出
    @Override
    public void process(Object[] args) throws HiveException {
        // 获取输入的参数: et
        String et = args[0].toString();
        if (!StringUtils.isBlank(et)) {
            try {
                JSONArray jsonArray = new JSONArray(et);
                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject event = jsonArray.getJSONObject(i);

                    String eventName = event.getString("en");
                    String eventJson = event.toString();
                    forward(new String[]{eventName, eventJson});
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
/*
[
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

create function base_analizer as 'com.atguigu.hive.udf.BaseFieldUDF' using jar 'hdfs://hadoop102:9000/user/hive/jars/hive-1.0-SNAPSHOT.jar';

create function flat_analizer as 'com.atguigu.hive.udtf.EventJsonUDTD' using jar 'hdfs://hadoop102:9000/user/hive/jars/hive-1.0-SNAPSHOT.jar';

insert overwrite table dwd_base_event_log partition(dt='2019-11-02')
select
    base_analizer(line, "mid") mid,
    base_analizer(line,'uid') as user_id,
    base_analizer(line,'vc') as version_code,
    base_analizer(line,'vn') as version_name,
    base_analizer(line,'l') as lang,
    base_analizer(line,'sr') as source,
    base_analizer(line,'os') as os,
    base_analizer(line,'ar') as area,
    base_analizer(line,'md') as model,
    base_analizer(line,'ba') as brand,
    base_analizer(line,'sv') as sdk_version,
    base_analizer(line,'g') as gmail,
    base_analizer(line,'hw') as height_width,
    base_analizer(line,'t') as app_time,
    base_analizer(line,'nw') as network,
    base_analizer(line,'ln') as lng,
    base_analizer(line,'la') as lat,
    event_type,
    event_json,
    base_analizer(line,'st') as server_time
from ods_event_log lateral view flat_analizer(base_analizer(line, "et")) tmp_flat as event_type, event_json
where dt='2019-11-02' and base_analizer(line, "et")!=''


 */