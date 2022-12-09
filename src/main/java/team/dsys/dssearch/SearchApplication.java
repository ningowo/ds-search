package team.dsys.dssearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.search.StoreEngine;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication(exclude= DataSourceAutoConfiguration.class)
public class SearchApplication {

    public static void main(String[] args) {
        SpringApplication.run(SearchApplication.class, args);
    }
}
