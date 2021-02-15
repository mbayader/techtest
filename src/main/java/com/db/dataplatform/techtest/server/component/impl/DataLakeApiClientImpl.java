package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.DataLakeApiClient;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

@Service
@Slf4j
public class DataLakeApiClientImpl implements DataLakeApiClient {

    private final String uriBankDataLake;
    private final RestTemplate restTemplate;

    public DataLakeApiClientImpl(@Value("${hadoop.uri}") String uriBankDataLake,
                                 RestTemplate restTemplate) {
        this.uriBankDataLake = uriBankDataLake;
        this.restTemplate = restTemplate;
    }

    @Retryable(value = HadoopClientException.class, maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}"))
    @Override
    public void save(DataEnvelope envelope) {
        log.info("Saving to DataLake.");
        try{
            restTemplate.postForEntity(
                    URI.create(uriBankDataLake),
                    new HttpEntity<>(envelope.getDataBody().getDataBody()),
                    HttpStatus.class);
        } catch (HttpServerErrorException ex) {
            log.error("Store to DataLake failed. {}", ex.getMessage(), ex);
            throw new HadoopClientException(ex.getMessage(), ex);
        }
    }
}
