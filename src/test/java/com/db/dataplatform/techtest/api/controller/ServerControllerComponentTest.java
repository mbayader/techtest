package com.db.dataplatform.techtest.api.controller;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.client.component.Client;
import com.db.dataplatform.techtest.server.api.controller.ServerController;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

import static com.db.dataplatform.techtest.TestDataHelper.TEST_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@TestPropertySource(properties = "{data.uri:http://some-server:8090/dataserver}")
public class ServerControllerComponentTest {

	private static final String URI_DATA = "http://some-server:8090/dataserver";
	private static final String URI_PUSHDATA = URI_DATA + "/pushdata";
	private static final String URI_GETDATA  = URI_DATA + "/data/{blockType}";
	private static final String URI_PUTDATA  = URI_DATA + "/update/{name}";

	@MockBean
	private Client client;
	@MockBean
	private Server serverMock;

	private DataEnvelope testDataEnvelope;
	private ObjectMapper objectMapper;
	private MockMvc mockMvc;
	@Autowired
	private ServerController serverController;
	@Autowired
	private WebApplicationContext context;

	@Before
	public void setUp() throws HadoopClientException, NoSuchAlgorithmException, IOException {
		mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
		objectMapper = Jackson2ObjectMapperBuilder
				.json()
				.build();

		testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();

		when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenReturn(true);
	}

	@Test
	public void testPushDataPostCallWorksAsExpected() throws Exception {

		String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

		MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
				.content(testDataEnvelopeJson)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(checksumPass).isTrue();
	}

	@Test
	public void testRetrieveDataGetCallWorksAsExpected() throws Exception {

		List<DataEnvelope> dataEnvelopes = Collections.singletonList(testDataEnvelope);
		String dataEnvelopesAsString = objectMapper.writeValueAsString(dataEnvelopes);

		BlockTypeEnum blockTypeA = BlockTypeEnum.BLOCKTYPEA;

		when(serverMock.retrieveDataEnvelope(eq(blockTypeA)))
				.thenReturn(dataEnvelopes);

		MvcResult mvcResult = mockMvc.perform(get(URI_GETDATA, blockTypeA.name())
				.content(dataEnvelopesAsString)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo(dataEnvelopesAsString);
	}

	@Test
	public void testUpdateDataPutCallWorksAsExpected() throws Exception {

		when(serverMock.updateDataBlockType(eq(TEST_NAME), eq(BlockTypeEnum.BLOCKTYPEA)))
				.thenReturn(true);

		MvcResult mvcResult = mockMvc.perform(put(URI_PUTDATA, TEST_NAME)
				.content(BlockTypeEnum.BLOCKTYPEA.name())
				.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andExpect(content().string("true"))
				.andReturn();

		boolean updateData = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(updateData).isTrue();
	}

	@Test
	public void testUpdateDataPutCallWorksAsExpectedWithInvalidName() throws Exception {

		mockMvc.perform(put(URI_PUTDATA, "	")
				.content(BlockTypeEnum.BLOCKTYPEA.name())
				.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isConflict())
				.andReturn();
	}
}
