package team.dsys.dssearch.util;

import com.alibaba.fastjson.JSON;

public class JsonUtil<T> {

    public static String objectToJSON(Object o) {
        return JSON.toJSONString(o);
    }

    public static <T> T jsonToObject(String s, Class<T> c) {
        return JSON.parseObject(s, c);
    }
}
