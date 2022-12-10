package team.dsys.dssearch.util;

/**
 * 32bits
 */
public class SnowflakeIDGenerator {

    public Integer generate() {
        String s = String.valueOf(System.currentTimeMillis());
        return (int) Long.parseLong(s) / Integer.MAX_VALUE;
    }

}
