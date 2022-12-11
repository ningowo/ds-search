package team.dsys.dssearch.util;

/**
 * 32bits
 */
public class SnowflakeIDGenerator {

    // generate global unique doc id
    public Integer generate() {
        return  (int) (System.currentTimeMillis() % Integer.MAX_VALUE);
    }

}
