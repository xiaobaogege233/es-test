package cn.zchd.test;

import cn.zchd.entity.HappinessrecordEntity;
import cn.zchd.entity.TestBean;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.CompositeAggregationSource;
import co.elastic.clients.elasticsearch._types.aggregations.CompositeBucket;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TrackHits;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.core.io.ClassPathResource;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.*;

@Slf4j
public class EsTest {

    public static void main(String[] args) {
//        String serverUrl = "127.0.0.1:9200";
        String serverUrl = "172.20.231.3:9200";
        String username = "elastic";
//        String password = "ZzW1guG*Mmosij9xmjaF";
        String password = "dsrrd@121018";
        ElasticsearchClient elasticsearchClient = connectByPasswordAndSsl(serverUrl, username, password);
        System.out.println(getCount(elasticsearchClient));
    }

    // 直连连接ES
    public static ElasticsearchClient connect(String serverUrl){
        ElasticsearchClient elasticsearchClient = null;
        if (StringUtils.isNoneBlank(serverUrl)) {
            RestClient client = RestClient.builder(Arrays.stream(serverUrl.split(",")).map(HttpHost::create).toArray(HttpHost[]::new)).build();
            ElasticsearchTransport transport = new RestClientTransport(client, new JacksonJsonpMapper());
            elasticsearchClient = new ElasticsearchClient(transport);
        }
        return elasticsearchClient;
    }

    // 使用账号密码和证书去连
    public static ElasticsearchClient connectByPasswordAndSsl(String serverUrl,String username,String password){
        if (!org.springframework.util.StringUtils.hasLength(serverUrl)) {
            throw new RuntimeException("invalid elasticsearch configuration");
        }

        String[] hostArray = serverUrl.split(",");
        HttpHost[] httpHosts = new HttpHost[hostArray.length];
        HttpHost httpHost;
        for (int i = 0; i < hostArray.length; i++) {
            String[] strings = hostArray[i].split(":");
            httpHost = new HttpHost(strings[0], Integer.parseInt(strings[1]), "https");
            httpHosts[i] = httpHost;
        }

        // 账号密码的配置
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        // 自签证书的设置，并且还包含了账号密码
        RestClientBuilder.HttpClientConfigCallback callback = httpAsyncClientBuilder -> httpAsyncClientBuilder
                .setSSLContext(buildSSLContext())
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .setDefaultCredentialsProvider(credentialsProvider);

        // 用builder创建RestClient对象
        RestClient client = RestClient
                .builder(httpHosts)
                .setHttpClientConfigCallback(callback)
                .build();

        RestClientTransport transport = new RestClientTransport(client, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private static SSLContext buildSSLContext() {
//        ClassPathResource resource = new ClassPathResource("http_ca.crt");
        ClassPathResource resource = new ClassPathResource("http_ca_online.crt");
        SSLContext sslContext = null;
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            try (InputStream is = resource.getInputStream()) {
                trustedCa = factory.generateCertificate(is);
            }
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            sslContext = sslContextBuilder.build();
        } catch (CertificateException | IOException | KeyStoreException | NoSuchAlgorithmException |
                 KeyManagementException e) {
            log.error("ES连接认证失败", e);
        }

        return sslContext;
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



    // 单条查询  和数据库精准查询不一样 会有误差
    public static void getDocument (ElasticsearchClient elasticsearchClient) {
        SearchResponse<TestBean> search;
        try {
            search = elasticsearchClient.search(s -> s.index("test")
                            .from(0)  // 相当于mysql的offset
                            .size(10) // 相当于mysql的size
                            ,
                    TestBean.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Hit<TestBean> hit: search.hits().hits()) {
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

    public static void distinctEsQuery(ElasticsearchClient elasticsearchClient){
        TermQuery termQuery = TermQuery.of(t -> t.field("businesstypeList").value("7"));

        WildcardQuery wildcardQuery = WildcardQuery.of(t -> t.field("zonecodeList").value(1509 + "*"));
        SearchRequest request = SearchRequest.of(searchRequest ->
                {
                    searchRequest.index("happiness")
                            .from(0)
                            .size(10);

                    searchRequest.aggregations("count",a -> a
                            .cardinality(h -> h
                                    .field("idcard")
                                    .precisionThreshold(40000)
                            )
                    );

                    // 如果有多个 .query 后面的 query 会覆盖前面的 query
                    searchRequest.query(query -> query
                                    .bool(bool -> {
                                        bool.must(new Query(termQuery));
                                        bool.must(new Query(wildcardQuery));
//                                        bool.must(new Query(TermQuery.of(t -> t.field("businesstypeList").value("1"))));
                                        return bool;
                                    })
                    ).trackTotalHits(t -> t.enabled(true));
                    return  searchRequest;
                }
        );
        SearchResponse<TestBean> searchResponse;
        try {
            searchResponse = elasticsearchClient.search(request, TestBean.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("去重的总条数有：{}", searchResponse.aggregations().get("count").cardinality().value());
        log.info("返回的总条数有：{}", searchResponse.hits().total().value());
        List<Hit<TestBean>> hitList = searchResponse.hits().hits();
        for (Hit<TestBean> hit : hitList) {
            log.info("== hit: {}, id: {}", hit.source(), hit.id());
        }
    }

    public static long getCount(ElasticsearchClient elasticsearchClient){
        CountResponse count;
        try {
            count = elasticsearchClient.count(CountRequest.of(t -> t.index("happiness")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return count.count();
    }

    public static void testData(ElasticsearchClient elasticsearchClient){
        try {
            elasticsearchClient.indices().create(c -> c
                    .index("happiness")
                    .mappings(m -> m
                            .properties("realname",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("idcard",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("businesstypeList",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("zonecodeList",z -> z
                                    .keyword(k -> k.index(true)))
                            .properties("savemonthList",z -> z
                                    .keyword(k -> k.index(true)))
                    )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void testUpdateDocument (ElasticsearchClient elasticsearchClient) {
        for (int i = 0; i < 10; i++) {
            SearchResponse<TestBean> search;
            try {
                search = elasticsearchClient.search(s -> s.index("test")
                                .size(1) // 相当于mysql的size
                                .query(q -> q.term(t -> t.field("idcard").value("421281199701240015")))
                        ,
                        TestBean.class);
                System.out.println(search.hits().hits().isEmpty());
                TestBean source = search.hits().hits().get(0).source();
                String id = search.hits().hits().get(0).id();
                UpdateResponse<TestBean> updateResponse = elasticsearchClient.update(updateRequest ->
                        updateRequest.index("test").id(id)
                                .doc(source), TestBean.class
                );
                // 建议强制刷新
                elasticsearchClient.indices().refresh(refreshRequest -> refreshRequest.index("test"));
                log.info("== response: {}, responseStatus: {}", updateResponse, updateResponse.result());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @describe 索引多个文档
     * @param index 索引名称
     * @param ls 文档列表
     * @return 索引文档结果
     * @throws Exception 抛出异常
     */
    private static BulkResponse bulk(ElasticsearchClient elasticsearchClient,String index, List<?> ls) {

        try {
            return elasticsearchClient.bulk(_0 -> {
                _0.index(index);
                ls.forEach(_1 -> _0
                        .operations(_2 -> _2
                                .create(_3 -> _3
                                        .document(_1))));
                return _0;
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
