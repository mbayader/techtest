package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
public class ClientImpl implements Client {

    private final RestTemplate restTemplate;
    private final String dataUri;

    private static final  String URI_PUSHDATA = "/pushdata";
    private static final  String URI_GETDATA = "/data/{blockType}";
    private static final  String URI_PUTDATA = "/update/{name}";

    public ClientImpl(RestTemplate restTemplate,
                      @Value("${data.uri}") String dataUri) {
        this.restTemplate = restTemplate;
        this.dataUri = dataUri;
    }

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);

        HttpEntity<DataEnvelope> request = new HttpEntity<>(dataEnvelope);
        createRestRequest(createUri(URI_PUSHDATA),
                HttpMethod.POST,
                request,
                new ParameterizedTypeReference<Boolean>() {});
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);

        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("blockType", blockType);

        return createRestRequest(createUri(URI_GETDATA,uriVariables),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<DataEnvelope>>() {});
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);

        HashMap<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("name", blockName);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);

        HttpEntity<String> request = new HttpEntity<>(newBlockType, headers);

        return createRestRequest(createUri(URI_PUTDATA, uriVariables),
                HttpMethod.PUT,
                request,
                new ParameterizedTypeReference<Boolean>() {});
    }

    private <T, R> R createRestRequest(URI uri, HttpMethod httpMethod,
                                       HttpEntity<T> request,
                                       ParameterizedTypeReference<R> responseType
    ) {
        ResponseEntity<R> response = restTemplate
                .exchange(uri, httpMethod, request, responseType);
        return response.getBody();
    }

    private URI createUri(String uri) {
        return createUri(uri, Collections.emptyMap());
    }

    private <T> URI createUri(String uri, Map<String, T> pathParam) {
        String uriTemplate = dataUri + uri;
        return pathParam.isEmpty() ?
                new UriTemplate(uriTemplate).expand() :
                new UriTemplate(uriTemplate).expand(pathParam);
    }
}
