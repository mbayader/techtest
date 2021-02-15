package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.client.component.Client;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.DataLakeApiClient;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class DataLakeApiClientImplTest {

    public static final DataEnvelope DATA_ENVELOPE = TestDataHelper.createTestDataEnvelopeApiObject();

    @MockBean
    private Client client;

    @MockBean
    private RestTemplate restTemplateMock;

    @Autowired
    private DataLakeApiClient dataLakeApiClient;

    @Test(expected = HadoopClientException.class)
    public void shouldThrowHadoopClientExceptionWhenRestCallFails() {
        when(restTemplateMock.postForEntity(
                any(URI.class),
                eq(new HttpEntity<>(DATA_ENVELOPE.getDataBody().getDataBody())),
                eq(HttpStatus.class)))
                .thenThrow(new HttpServerErrorException(HttpStatus.GATEWAY_TIMEOUT));

        dataLakeApiClient.save(DATA_ENVELOPE);
    }

    @Test
    public void shouldRetryRestCallsWhenFailedFirstAttempt() {

        when(restTemplateMock.postForEntity(
                any(URI.class),
                eq(new HttpEntity<>(DATA_ENVELOPE.getDataBody().getDataBody())),
                eq(HttpStatus.class)))
                .thenThrow(new HttpServerErrorException(HttpStatus.GATEWAY_TIMEOUT));

        assertThrows(HadoopClientException.class,
                () -> dataLakeApiClient.save(DATA_ENVELOPE));

        verify(restTemplateMock, times(2)).postForEntity(
                any(URI.class),
                eq(new HttpEntity<>(DATA_ENVELOPE.getDataBody().getDataBody())),
                eq(HttpStatus.class));
    }
}
