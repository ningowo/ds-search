package team.dsys.dssearch.model;


import lombok.Data;

@Data
public class Doc {
    
    String _index;

    long _id;

    String content;
}
