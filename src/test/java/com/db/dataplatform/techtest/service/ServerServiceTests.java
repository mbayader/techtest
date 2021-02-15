package com.db.dataplatform.techtest.service;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.mapper.DataEnvelopeConverter;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.component.impl.ServerImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.modelmapper.ModelMapper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.db.dataplatform.techtest.TestDataHelper.createTestDataBodyEntity;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObjectWithInvalidChecksum;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataHeaderEntity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ServerServiceTests {

    @Mock
    private DataBodyService dataBodyServiceMock;

    private ModelMapper modelMapper;

    private DataBodyEntity expectedDataBodyEntity;
    private DataEnvelope testDataEnvelope;

    private Server server;

    @Before
    public void setup() {
        ServerMapperConfiguration serverMapperConfiguration = new ServerMapperConfiguration();
        modelMapper = serverMapperConfiguration.createModelMapperBean(Collections.singleton(new DataEnvelopeConverter()));

        testDataEnvelope = createTestDataEnvelopeApiObject();
        expectedDataBodyEntity = modelMapper.map(testDataEnvelope.getDataBody(), DataBodyEntity.class);
        expectedDataBodyEntity.setDataHeaderEntity(modelMapper.map(testDataEnvelope.getDataHeader(), DataHeaderEntity.class));

        server = new ServerImpl(dataBodyServiceMock, modelMapper);
    }

    @Test
    public void shouldSaveDataEnvelopeAsExpected() throws NoSuchAlgorithmException, IOException {
        boolean success = server.saveDataEnvelope(testDataEnvelope);

        assertThat(success).isTrue();
        verify(dataBodyServiceMock, times(1)).saveDataBody(eq(expectedDataBodyEntity));
    }

    @Test
    public void shouldNotSaveDataEnvelopeWhenCheckSumNotMatch() throws NoSuchAlgorithmException, IOException {
        DataEnvelope testDataEnvelopeWithNotMatchChecksum = createTestDataEnvelopeApiObjectWithInvalidChecksum();

        boolean failed = server.saveDataEnvelope(testDataEnvelopeWithNotMatchChecksum);

        assertThat(failed).isFalse();
        verify(dataBodyServiceMock, never()).saveDataBody(any(DataBodyEntity.class));
    }

    @Test
    public void shouldRetrieveDataEnvelopeByBlockType() throws NoSuchAlgorithmException, IOException{
        BlockTypeEnum testBlockTypeA = BlockTypeEnum.BLOCKTYPEA;
        DataBodyEntity testDataBodyEntity = createTestDataBodyEntity(createTestDataHeaderEntity(Instant.now()));

        when(dataBodyServiceMock.getDataByBlockType(testBlockTypeA))
                .thenReturn(Collections.singletonList(testDataBodyEntity));

        List<DataEnvelope> results = server.retrieveDataEnvelope(testBlockTypeA);

        assertThat(results).containsExactly(testDataEnvelope);
        verify(dataBodyServiceMock, times(1)).getDataByBlockType(testBlockTypeA);
    }
}
