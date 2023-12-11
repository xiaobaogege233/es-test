package cn.zchd.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;

@RestController
@Slf4j
public class EsIndexController {

    @Resource(name="clientByPasswd")
    private ElasticsearchClient elasticsearchClient;

    @RequestMapping("getAllIndex")
    public void getAllIndex() throws IOException {
        // 查看所有索引
        GetIndexResponse getIndexResponse = elasticsearchClient.indices().get(demo -> demo.index("*"));
        log.info(String.format("all index:%s",String.join(",", getIndexResponse.result().keySet())));
    }

    @RequestMapping("indexExists")
    public boolean indexExists() throws IOException {
        return elasticsearchClient.indices().exists(b -> b.index("happinessrecord")).value();
    }
}
