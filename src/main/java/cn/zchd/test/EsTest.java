package cn.zchd.test;

import cn.zchd.entity.HappinessrecordEntity;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class EsTest {

    public static void main(String[] args) {
        // 连接ES
        String elasticsearchUris = "127.0.0.1:9200";
        ElasticsearchClient elasticsearchClient = null;
        if (StringUtils.isNoneBlank(elasticsearchUris)) {
            RestClient client = RestClient.builder(Arrays.stream(elasticsearchUris.split(",")).map(HttpHost::create).toArray(HttpHost[]::new)).build();
            ElasticsearchTransport transport = new RestClientTransport(client, new JacksonJsonpMapper());
            elasticsearchClient = new ElasticsearchClient(transport);
        }
//        indexDetail(elasticsearchClient);
//        createIndexAndMappings(elasticsearchClient);
//        deleteIndex(elasticsearchClient);
//        indexIsExist(elasticsearchClient);
//        getDocument(elasticsearchClient);
        testMultipleCondition(elasticsearchClient);
    }

    // 创建索引并且映射字段
    public static void createIndexAndMappings(ElasticsearchClient elasticsearchClient){
        try {
            elasticsearchClient.indices().create(c -> c
                    .index("happiness")
                    .mappings(t -> t
                            .properties("id",z -> z
                                    .long_(l -> l.index(true)))
                            .properties("businesstype",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("savemonth",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("zonecode",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("address",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("realname",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("idcard",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("amount",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("createtime",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("businesstypestr",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("headname",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("headcard",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("membercount",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("starttime",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("endtime",z -> z
                                    .keyword(k -> k.index(true)))

                    )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 部分聚合统计操作
    public static void queryCount(ElasticsearchClient elasticsearchClient){
        try {
            // 模糊查询
            WildcardQuery wildcardQuery = WildcardQuery.of(t -> {
                t.field("zonecode").value("1509" + "*");
                return t;
            });
            // 范围查询
            RangeQuery rangeQuery = RangeQuery.of(t -> {
                t.field("savemonth").gte(JsonData.of("2023-01")).lte(JsonData.of("2023-03"));
                return t;
            });
            // 等价查询
            TermQuery termQuery = TermQuery.of(t -> {
                t.field("realname").value("吴秀兰");
                return t;
            });

            BoolQuery boolQuery = BoolQuery.of(t -> {
                t.must(new Query(wildcardQuery));
                t.must(new Query(rangeQuery));
                t.must(new Query(termQuery));
                return t;
            });

            SearchResponse<Void> search = elasticsearchClient.search(b-> b
                            .index("happinessrecord")
                            .size(0)
                            .query(boolQuery._toQuery())
                            .aggregations("unique_idcard_count", a -> a
                                    .cardinality(h -> h
                                            .field("idcard")
                                            .precisionThreshold(40000)
                                    )
                            ),
                    Void.class
            );
            long firstBucketCount = search.aggregations()
                    .get("unique_idcard_count")
                    .cardinality().value();
            System.out.println(firstBucketCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 查看索引的相关信息
    public static void indexDetail (ElasticsearchClient elasticsearchClient) {
        GetIndexResponse getIndexResponse;
        try {
            getIndexResponse = elasticsearchClient.indices()
                    .get(getIndexRequest ->
                            getIndexRequest.index("happiness")
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, Property> properties = getIndexResponse.get("happiness").mappings().properties();

        for (String key : properties.keySet()) {
            log.info("== {} 索引的详细信息为: == key: {}, Property: {}", "elasticsearch-client", key, properties.get(key)._kind());
        }

    }

    // 删除索引
    public static void deleteIndex (ElasticsearchClient elasticsearchClient) {
        DeleteIndexResponse deleteIndexResponse;
        try {
            deleteIndexResponse = elasticsearchClient.indices()
                    .delete(deleteIndexRequest ->
                            deleteIndexRequest.index("happiness")
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("== {} 索引创建是否删除成功: {}", "elasticsearch-client", deleteIndexResponse.acknowledged());
    }

    // 判断索引是否存在
    public static void indexIsExist (ElasticsearchClient elasticsearchClient) {
        BooleanResponse booleanResponse;
        try {
            booleanResponse = elasticsearchClient.indices()
                    .exists(existsRequest ->
                            existsRequest.index("happiness")
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("== {} 索引创建是否存在: {}", "elasticsearch-client", booleanResponse.value());
    }



    // 单挑查询  和数据库精准查询不一样 会有误差
    public static void getDocument (ElasticsearchClient elasticsearchClient) {
        SearchResponse<HappinessrecordEntity> search;
        try {
            search = elasticsearchClient.search(s -> s.index("happinessrecord")
                            .from(0)  // 相当于mysql的offset
                            .size(10) // 相当于mysql的size
                            .query(q ->
                                    q.term(t ->
                                            t.field("realname").value(v -> v.stringValue("吴秀兰"))
                                    )
                            ),
                    HappinessrecordEntity.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Hit<HappinessrecordEntity> hit: search.hits().hits()) {
            log.info("== hit: source: {}, id: {}", hit.source(), hit.id());
        }
    }

    // 多条件 返回查询 和数据库精准查询不一样 会有误差
    public static void testMultipleCondition (ElasticsearchClient elasticsearchClient)  {

        SearchRequest request = SearchRequest.of(searchRequest ->
                searchRequest.index("happinessrecord")
                        .from(0)
                        .size(20)
//                        .sort(s -> s.field(f -> f.field("age").order(SortOrder.Desc)))
                        // 如果有多个 .query 后面的 query 会覆盖前面的 query
                        .query(query ->
                                query.bool(boolQuery ->
                                        boolQuery
                                                // 在同一个 boolQuery 中 must 会将 should 覆盖
                                                .must(must -> must.term(
//                                                        e -> e.field("age").gte(JsonData.of("21")).lte(JsonData.of("25"))
                                                        e -> e.field("realname").value("吴秀兰")
                                                ))
//                                                .mustNot(mustNot -> mustNot.term(
//                                                        e -> e.field("name").value(value -> value.stringValue("lisi1"))
//                                                ))
//                                                .should(must -> must.term(
//                                                        e -> e.field("name").value(value -> value.stringValue("lisi2"))
//                                                ))
                                )
                        )

        );

        SearchResponse<HappinessrecordEntity> searchResponse;
        try {
            searchResponse = elasticsearchClient.search(request, HappinessrecordEntity.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        log.info("返回的总条数有：{}", searchResponse.hits().total().value());
        List<Hit<HappinessrecordEntity>> hitList = searchResponse.hits().hits();
        for (Hit<HappinessrecordEntity> hit : hitList) {
            log.info("== hit: {}, id: {}", hit.source(), hit.id());
        }

    }
}
