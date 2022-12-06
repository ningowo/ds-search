package team.dsys.dssearch.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil<T> {

    private static ObjectMapper mapper = new ObjectMapper();

    public static String objectToJSON(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T> T jsonToObject(String s, Class<T> c) {
        try {
            return mapper.readValue(s, c);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
