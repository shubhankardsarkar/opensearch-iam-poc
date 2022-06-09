package com.bh.vc.opensearchiampoc.config;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

/**
 * AWS SDK v2 version of: https://github.com/awslabs/aws-request-signing-apache-interceptor/blob/master/src/main/java/com/amazonaws/http/AWSRequestSigningApacheInterceptor.java
 */
@RequiredArgsConstructor
public class AWSRequestSigningInterceptor implements HttpRequestInterceptor {

  private Aws4Signer signer;
  private Aws4SignerParams params;

  public AWSRequestSigningInterceptor(Aws4Signer signer2, Aws4SignerParams params2) {
	this.signer = signer2;
	this.params = params2;
}

@Override
  public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
    URIBuilder uriBuilder;
    try {
      uriBuilder = new URIBuilder(request.getRequestLine().getUri());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI" , e);
    }

    final SdkHttpFullRequest.Builder signableRequestBuilder = SdkHttpFullRequest.builder();

    final HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
    if (host != null) {
      signableRequestBuilder.uri(URI.create(host.toURI()));
    }
    final SdkHttpMethod httpMethod =
      SdkHttpMethod.fromValue(request.getRequestLine().getMethod());
    signableRequestBuilder.method(httpMethod);
    try {
      signableRequestBuilder.encodedPath(uriBuilder.build().getRawPath());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI" , e);
    }

    if (request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest httpEntityEnclosingRequest =
        (HttpEntityEnclosingRequest) request;
      if (httpEntityEnclosingRequest.getEntity() != null) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        httpEntityEnclosingRequest.getEntity().writeTo(outputStream);
        signableRequestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(outputStream.toByteArray()));
      }
    }

    // Append Parameters and Headers
    nvpToMapParams(uriBuilder.getQueryParams()).forEach(signableRequestBuilder::appendRawQueryParameter);
    headerArrayToMap(request.getAllHeaders()).forEach(signableRequestBuilder::appendHeader);

    // Sign it
    final SdkHttpFullRequest signedRequest = signer.sign(signableRequestBuilder.build(), params);

    // Now copy everything back
    request.setHeaders(mapToHeaderArray(signedRequest.headers()));
    if (request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest httpEntityEnclosingRequest =
        (HttpEntityEnclosingRequest) request;
      if (httpEntityEnclosingRequest.getEntity() != null) {
        BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
        Optional<ContentStreamProvider> contentStreamProvider = signedRequest.contentStreamProvider();
        if (contentStreamProvider.isPresent()) {
          basicHttpEntity.setContent(contentStreamProvider.get().newStream());
        } else {
          throw new RuntimeException("Empty content stream was not expected!");
        }
        httpEntityEnclosingRequest.setEntity(basicHttpEntity);
      }
    }
  }

  /**
   *
   * @param params list of HTTP query params as NameValuePairs
   * @return a Multimap of HTTP query params
   */
  private static Map<String, String> nvpToMapParams(final List<NameValuePair> params) {
    Map<String, String> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (NameValuePair nvp : params) {
      parameterMap.putIfAbsent(nvp.getName(), nvp.getValue());
    }
    return parameterMap;
  }

  /**
   * @param headers modeled Header objects
   * @return a Map of header entries
   */
  private static Map<String, String> headerArrayToMap(final Header[] headers) {
    Map<String, String> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (Header header : headers) {
      if (!skipHeader(header)) {
        headersMap.put(header.getName(), header.getValue());
      }
    }
    return headersMap;
  }

  /**
   * @param header header line to check
   * @return true if the given header should be excluded when signing
   */
  private static boolean skipHeader(final Header header) {
    return ("content-length".equalsIgnoreCase(header.getName())
      && "0".equals(header.getValue())) // Strip Content-Length: 0
      || "host".equalsIgnoreCase(header.getName()); // Host comes from endpoint
  }

  /**
   * @param mapHeaders Map of header entries
   * @return modeled Header objects
   */
  private static Header[] mapToHeaderArray(final Map<String, List<String>> mapHeaders) {
    Header[] headers = new Header[mapHeaders.size()];
    int i = 0;
    for (Map.Entry<String, List<String>> headerEntry : mapHeaders.entrySet()) {
      for (String value : headerEntry.getValue()) {
        headers[i++] = new BasicHeader(headerEntry.getKey(), value);
      }
    }
    return headers;
  }

}