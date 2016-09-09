package org.mrgeo.publisher.rest;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by ericwood on 8/11/16.
 */
public class RestClient {

    private String baseUrl;
    private Executor executor = Executor.newInstance();

    public RestClient(String baseUrl) {
        // Remove trailing slash if present
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        this.baseUrl = baseUrl;
    }

    /**
     * Use this constructor if the server requires authentication
     * @param baseUrl
     * @param username
     * @param password
     */
    public RestClient(String baseUrl, String username, String password) {
        this(baseUrl);
        executor.auth(username, password);
    }

    public String getJson(String url) throws IOException, HttpResponseException,
            ClientProtocolException {
        return execute(Request.Get(getFullUrl(url))).handleResponse(new JsonResponseHandler());
    }

    /**
     * Make a GET request that returns a JSON string
     * @param url The url after the baseUrl
     * @param requestHeaders
     * @return
     * @throws IOException
     * @throws HttpResponseException
     * @throws ClientProtocolException
     */
    public String getJson(String url, Map<String, String> requestHeaders) throws IOException, HttpResponseException,
            ClientProtocolException {
        return execute(setHeaders(Request.Get(getFullUrl(url)), requestHeaders)).handleResponse(new JsonResponseHandler());
    }

    public String postJson(String url, String jsonEntity) throws IOException, HttpResponseException,
            ClientProtocolException {
        return postPutJson(Request.Post(getFullUrl(url)), jsonEntity);
    }

    /**
     * Make a POST request that includes a JSON object
     * @param url The url after the baseUrl
     * @param requestHeaders
     * @return
     * @throws IOException
     * @throws HttpResponseException
     * @throws ClientProtocolException
     */
    public String postJson(String url, String jsonEntity, Map<String, String> requestHeaders) throws IOException, HttpResponseException,
            ClientProtocolException {
        return postPutJson(Request.Post(getFullUrl(url)), jsonEntity, requestHeaders);
    }

    public String putJson(String url, String jsonEntity) throws IOException, HttpResponseException,
            ClientProtocolException {
        return postPutJson(Request.Put(getFullUrl(url)), jsonEntity);
    }

    /**
     * Make a PUT request that includes a JSON object
     * @param url The url after the baseUrl
     * @param requestHeaders
     * @return
     * @throws IOException
     * @throws HttpResponseException
     * @throws ClientProtocolException
     */
    public String putJson(String url, String jsonEntity, Map<String, String> requestHeaders) throws IOException, HttpResponseException,
            ClientProtocolException {
        return postPutJson(Request.Put(getFullUrl(url)), jsonEntity, requestHeaders);
    }

    private String postPutJson(Request request, String jsonEntity) throws IOException, HttpResponseException,
            ClientProtocolException {
        return execute(request.bodyString(jsonEntity, ContentType.APPLICATION_JSON)).handleResponse(new ResponseHandler());
    }

    private String postPutJson(Request request, String jsonEntity, Map<String, String> requestHeaders) throws IOException, HttpResponseException,
            ClientProtocolException {
        return execute(setHeaders(request, requestHeaders).bodyString(jsonEntity, ContentType.APPLICATION_JSON))
                                                          .handleResponse(new ResponseHandler());
    }

    private String getFullUrl(String urlEnd) {
        // Remove leading slash if present
        if (urlEnd.startsWith("/")) {
            urlEnd = urlEnd.substring(1);
        }
        return baseUrl + "/" + urlEnd;
    }

    private Response execute(Request request) throws IOException {
        // Execute the request using the executor so the auth information is included
        return executor.execute(request);
    }

    private static Request setHeaders(Request request, Map<String,String> requestHeaders) {
        for (Map.Entry<String,String> headerEntry : requestHeaders.entrySet()) {
            request.setHeader(headerEntry.getKey(),headerEntry.getValue());
        }
        return request;
    }


    private static String getResponseAsJson(HttpResponse response) throws IOException, HttpResponseException,
                                                                          ClientProtocolException {
        return getResponse(response, ContentType.APPLICATION_JSON);
    }

    private static String getResponse(HttpResponse response) throws IOException, HttpResponseException,
            ClientProtocolException {
        return getResponse(response, null);
    }

    private static String getResponse(HttpResponse response, ContentType expectedContentType) throws IOException, HttpResponseException,
            ClientProtocolException {
        StringBuilder sbJson = new StringBuilder();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                getResponseContent(response, expectedContentType)));
        try {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                sbJson.append(inputLine + "\n");
            }
            in.close();
            return sbJson.toString();
        }
        finally {
            in.close();
        }
    }

    private static InputStream getResponseContent(HttpResponse response, ContentType expectedContentType) throws
            IOException, HttpResponseException, ClientProtocolException {
        StatusLine statusLine = response.getStatusLine();
        HttpEntity entity = response.getEntity();
        int responseCode = statusLine.getStatusCode();
        if ((responseCode >= 300) ) {
            throw new HttpResponseException(responseCode, statusLine.getReasonPhrase());
        }
        if (entity == null) {
            throw new ClientProtocolException("Response contains no content");
        }
        if (expectedContentType != null) {
            ContentType contentType = ContentType.getOrDefault(entity);
            if (!contentType.getMimeType().equals(expectedContentType.getMimeType())) {
                throw new ClientProtocolException("Unexpected content type:" +
                        contentType);
            }
        }
        return entity.getContent();

    }

    private static class JsonResponseHandler implements org.apache.http.client.ResponseHandler<String> {
        public String handleResponse(final HttpResponse response) throws IOException, HttpResponseException,
                ClientProtocolException {
            return getResponseAsJson(response);
        }
    }

    private static class ResponseHandler implements org.apache.http.client.ResponseHandler<String> {
        public String handleResponse(final HttpResponse response) throws IOException, HttpResponseException,
                ClientProtocolException {
            return getResponse(response);
        }
    }
}
