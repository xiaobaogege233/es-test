package cn.zchd.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class ElasticSearchConfig {

    @Value("${spring.elasticsearch.uris}")
    private String elasticsearchUris;

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        if (StringUtils.isNoneBlank(elasticsearchUris)) {
            RestClient client = RestClient.builder(Arrays.stream(elasticsearchUris.split(",")).map(HttpHost::create).toArray(HttpHost[]::new)).build();
            ElasticsearchTransport transport = new RestClientTransport(client, new JacksonJsonpMapper());
            return new ElasticsearchClient(transport);
        } else {
            throw new RuntimeException("未读取到es的uri配置信息");
        }
    }
}
