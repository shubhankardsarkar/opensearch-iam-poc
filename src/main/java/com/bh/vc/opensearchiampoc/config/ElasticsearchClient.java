package com.bh.vc.opensearchiampoc.config;

import com.bh.vc.opensearchiampoc.config.AWSRequestSigningInterceptor;
import com.bh.vc.opensearchiampoc.config.ElasticsearchClient;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.regions.Region;

@Slf4j
public class ElasticsearchClient {
	private static final Logger log = LoggerFactory.getLogger(ElasticsearchClient.class);
    private static final String service = "es";
    private static final Region region = Region.US_EAST_1;
	
    public static RestHighLevelClient getClient(DefaultCredentialsProvider credentialsProvider, String elasticsearchHost) {
		log.info("Create RestHighLevelClient client from configuration");

		log.info("Get credential");
		AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();

		log.info("credentialsProviderContainer: " + awsCredentials.toString());
		log.info("accessKeyId:" + awsCredentials.accessKeyId());
		log.info("secretAccessKey:" + awsCredentials.secretAccessKey());

		log.info("Create Aws4Signer");
		Aws4Signer signer = Aws4Signer.create();
		Aws4SignerParams params = Aws4SignerParams.builder()
				.awsCredentials(awsCredentials)
				.signingName(service)
				.signingRegion(region)
				.build();

		log.info("AWSRequestSigningInterceptor");
		AWSRequestSigningInterceptor interceptor = new AWSRequestSigningInterceptor(signer, params);

		log.info("Return client");
		// Adds the interceptor to the Elasticsearch REST client
		RestHighLevelClient restClient = new RestHighLevelClient(RestClient
				.builder(new HttpHost(elasticsearchHost, 443, "https"))
				.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor))
		);
		return restClient;

	}

}
