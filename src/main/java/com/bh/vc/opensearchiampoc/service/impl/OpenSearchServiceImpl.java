package com.bh.vc.opensearchiampoc.service.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.bh.vc.opensearchiampoc.config.ElasticsearchClient;
import com.fasterxml.jackson.core.JsonProcessingException;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

@Component
public class OpenSearchServiceImpl {

	@Value("${elasticsearch.host}")
	private String elasticsearchHost;
	
	@Value("${elasticsearch.index}")
	private String elasticsearchIndex;
	
	@Autowired
	private RestHighLevelClient restHighLevelClient;
	
	private static final Logger logger = LoggerFactory.getLogger(OpenSearchServiceImpl.class);
	
	private static final String SEARCH_ENDPOINT = "/_search";
	
	public ResponseEntity<String> search(String body) throws IOException {
		return search(body, null);
	}
	
	public ResponseEntity<String> search(String body, String metaIndex) throws IOException {
		String query = "{\r\n"
				+ "  \"size\": 10,\r\n"
				+ "  \"query\": {\r\n"
				+ "    \"multi_match\": {\r\n"
				+ "      \"query\": \"0001361897\",\r\n"
				+ "      \"fields\": [\"ge_SALES_ORDER\",\"mat_NO\"],\r\n"
				+ "      \"operator\": \"or\"\r\n"
				+ "    }\r\n"
				+ "  }\r\n"
				+ "}";
		//(body.getDslQuery() instanceof String) ? (String) body.getDslQuery() : Util.asJsonString(body.getDslQuery());
		
		
		return this.executeQuery(query, SEARCH_ENDPOINT, metaIndex, null);
	}

	private ResponseEntity<String> executeQuery(String query, String endpoint, String metaIndex, String methodAction) {
		logger.info("[START]: ExecuteQuery for elasticSearch.");
		final String POST_METHOD = "POST";

		String index = metaIndex == null ? elasticsearchIndex : metaIndex;
		final String FULL_ENDPOINT = "/" + index + endpoint;


		logger.info("[REQUEST] executeQuery(): Method [{}], Endpoint [{}], Body [{}]", POST_METHOD, FULL_ENDPOINT,
				query);

		Request request = new Request(POST_METHOD, FULL_ENDPOINT);
		request.setJsonEntity(query);

		ResponseEntity<String> responseEntity;

		DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
		try(RestHighLevelClient client = getRestHighLevelClient(credentialsProvider)) {

			Response response = client.getLowLevelClient().performRequest(request);

			StatusLine statusLine = response.getStatusLine();
			Integer statusCode = statusLine.getStatusCode();
			String responseContent = EntityUtils.toString(response.getEntity());

			logger.info("[RESPONSE] executeQuery(): Headers [{}], ReasonPhrase [{}], StatusCode [{}], Body [{}]",
					Arrays.toString(response.getHeaders()), statusLine.getReasonPhrase(), statusCode, responseContent);

			responseEntity = ResponseEntity.status(statusCode).contentType(MediaType.APPLICATION_JSON)
					.body(responseContent);

		} catch (Exception e) {
			logger.error("executeQuery(): Exception has been raised: ", e);
			responseEntity = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		logger.info("[END]: ExecuteQuery for elasticSearch.");
		return responseEntity;
	}
	
	public ResponseEntity<String> getBySysId(String sysId , String metaIndex) throws JsonProcessingException {
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.termQuery("_id", sysId));

//		SearchRequest searchRequest = new SearchRequest(metaIndex == null || metaIndex.equals(Constants.FLEET_DENORM) ? elasticsearchIndex : metaIndex);
		SearchRequest searchRequest = new SearchRequest("orders");
		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse;

		DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
		try(RestHighLevelClient client = getRestHighLevelClient(credentialsProvider)) {

			searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			restHighLevelClient.close();

		} catch (Exception e) {
			logger.error("Exception has been raised: ", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

		return ResponseEntity.ok(searchResponse.toString());
	}
	
	public RestHighLevelClient getRestHighLevelClient(DefaultCredentialsProvider credentialsProvider){
		return ElasticsearchClient.getClient(credentialsProvider, elasticsearchHost);
	}
}
