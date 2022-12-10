package team.dsys.dssearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import team.dsys.dssearch.rpc.Doc;
import team.dsys.dssearch.search.StoreEngine;
import team.dsys.dssearch.server.ThriftServer;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication(exclude= DataSourceAutoConfiguration.class)
public class SearchApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(SearchApplication.class, args);
        ThriftServer server = context.getBean(ThriftServer.class);
        server.start();
    }

}
