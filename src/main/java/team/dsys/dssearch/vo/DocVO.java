package team.dsys.dssearch.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.web.bind.annotation.GetMapping;

@Setter
@Getter
@AllArgsConstructor
public class DocVO {

    public int index;

    public int id;

    public String content;

}
