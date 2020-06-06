package com.zing.es.sample.api;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.zing.es.sample.sample.SampleService;



@RestController
@RequestMapping("/web/es")
public class EsController {
	
	@Autowired 
	SampleService sampleService;
	
	@GetMapping("list")
	public List list(String keyWords){
		String indexName = "customerbrife*";
		String result = "";
//		boolean flag = sampleService.testEsRestClient(city);
//		String keyWords = "美莱";
		List<String> filedList = new ArrayList<String>();
		filedList.add("customer_name");
		filedList.add("mobile");
		filedList.add("member_card_num");
		//根据关键词多字段查询
		List rsList = sampleService.multiFieldByByKeyWords(indexName,keyWords, filedList);
		for (Object rs : rsList) {
			System.out.println(rs);
		}
		return rsList;
	}
	
}
