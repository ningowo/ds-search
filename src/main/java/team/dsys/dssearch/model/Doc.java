package team.dsys.dssearch.model;


import lombok.Data;

@Data
public class Doc {
    
    String _index;

    public long get_id() {
        return _id;
    }

    long _id;

    String content;
}
