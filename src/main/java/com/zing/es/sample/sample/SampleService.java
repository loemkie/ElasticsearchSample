package com.zing.es.sample.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zing
 * Date 2019-01-31
 */
@Slf4j
@Service
public class SampleService {

    /**
     * 名称与Config中Bean的名称一致
     */
    @Autowired
    RestHighLevelClient highLevelClient;

   
    /**
     * 使用方式
     *
     * @return test is passed
     */
    public boolean testEsRestClient(String indexName,String city) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("city", city));
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Arrays.stream(response.getHits().getHits())
                    .forEach(i -> {
                        log.info(i.getIndex());
                        log.info(i.getSourceAsString());
                        log.info(i.getType());
                    });
            log.info("total:{}", response.getHits().totalHits);
            List resultList = responseToList(response);
            log.info(resultList.toString());
            return true;
        } catch (IOException e) {
            log.error("test failed", e);
            return false;
        }
    }
    
    /**
     * 根据布尔条件进行查询
     * @param boolQueryBuilder
     * @return
     */
    public SearchResponse searchMessage(String indexName,BoolQueryBuilder boolQueryBuilder) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.size(100);
            sourceBuilder.query(boolQueryBuilder);
            log.info(sourceBuilder.toString());
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(message -> {
                try {
                    String sourceAsString = message.getSourceAsString();
                    log.info(sourceAsString);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            return searchResponse;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Couldn't get Detail");
        }
    }

    /**
     * 单条件检索
     * @param fieldKey
     * @param fieldValue
     * @return
     */
    public MatchPhraseQueryBuilder uniqueMatchQuery(String fieldKey,String fieldValue){
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery(fieldKey,fieldValue);
        return matchPhraseQueryBuilder;
    }

    /**
     * 多条件检索并集，适用于搜索比如包含腾讯大王卡，滴滴大王卡的用户
     * @param fieldKey
     * @param queryList
     * @return
     */
    public BoolQueryBuilder orMatchUnionWithList(String fieldKey,List<String> queryList){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (String fieldValue : queryList){
            boolQueryBuilder.should(QueryBuilders.matchPhraseQuery(fieldKey,fieldValue));
        }
        return boolQueryBuilder;
    }

    /**
     * 多字段查询
     * @param fieldKey
     * @param queryList
     * @return
     */
    public BoolQueryBuilder multiFiledQuery(String keyWorkds,List<String> filedList){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (String key : filedList){
            boolQueryBuilder.should(QueryBuilders.wildcardQuery(key,"*"+keyWorkds+"*"));
        }
        return boolQueryBuilder;
    }
    /**
     * 范围查询，左右都是闭集
     * @param fieldKey
     * @param start
     * @param end
     * @return
     */
    public RangeQueryBuilder rangeMathQuery(String fieldKey,String start,String end){
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(fieldKey);
        rangeQueryBuilder.gte(start);
        rangeQueryBuilder.lte(end);
        return rangeQueryBuilder;
    }

    /**
     * 根据中文分词进行查询
     * @param fieldKey
     * @param fieldValue
     * @return
     */
    public MatchQueryBuilder matchQueryBuilder(String fieldKey,String fieldValue){
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(fieldKey,fieldValue).analyzer("ik_smart");
        return matchQueryBuilder;
    }

    /**
     * 根据关键词多字段查询
     * @param keyWords
     * @param filedList
     * @return
     */
    public List multiFieldByByKeyWords(String indexName,String keyWords,List <String>filedList){
    	BoolQueryBuilder boolQueryBuilder = this.multiFiledQuery(keyWords, filedList);
		SearchResponse searchResponse =  this.searchMessage(indexName,boolQueryBuilder);
		List<String> rs = this.responseToList(searchResponse);
		return rs;
    }
    /**
	 * 将查询后获得的response转成list
	 * @param client
	 * @param response
	 * @return
	 */
	public static List responseToList(SearchResponse response){
		SearchHits hits = response.getHits();
		List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
		for (int i = 0; i < hits.getHits().length; i++) {
			Map<String, Object> map = hits.getAt(i).getSourceAsMap();
			list.add(map);
		}
		return list;
	}


}
