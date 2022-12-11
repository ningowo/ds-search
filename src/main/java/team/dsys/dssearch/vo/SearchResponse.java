package team.dsys.dssearch.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import team.dsys.dssearch.rpc.Doc;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class SearchResponse {

    private int status;

    private String msg;

    private List<DocVO> docList;
}