package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    private final RestTemplate restTemplate;

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PUTDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}");

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);

        HttpEntity<DataEnvelope> request = new HttpEntity<>(dataEnvelope);
        restTemplate.exchange(
                new UriTemplate(URI_PUSHDATA).expand(),
                HttpMethod.POST,
                request,
                new ParameterizedTypeReference<Boolean>() {});
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);

        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("blockType", blockType);

        return restTemplate.exchange(
                URI_GETDATA.expand(uriVariables),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<DataEnvelope>>() {})
                .getBody();
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);

        HashMap<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("name", blockName);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);

        HttpEntity<String> request = new HttpEntity<>(newBlockType, headers);

        return restTemplate.exchange(URI_PUTDATA.expand(uriVariables),
                HttpMethod.PUT,
                request,
                new ParameterizedTypeReference<Boolean>() {})
                .getBody();
    }
}
