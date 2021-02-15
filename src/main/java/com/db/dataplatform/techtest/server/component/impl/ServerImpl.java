package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.db.dataplatform.techtest.TechTestApplication.MD5_CHECKSUM;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyService;
    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) {

        String calculateMd5 = calculateMd5(envelope.getDataBody().getDataBody());

        if (MD5_CHECKSUM.equals(calculateMd5)) {
            // Save to persistence.
            persist(envelope);
            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
            return true;
        }
        return false;
    }

    private String calculateMd5(String dataBody) {
        return DigestUtils.md5Hex(dataBody);
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyService.saveDataBody(dataBodyEntity);
    }

    @Override
    public List<DataEnvelope> retrieveDataEnvelope(BlockTypeEnum blockType) {
        List<DataBodyEntity> byBlockType = dataBodyService.getDataByBlockType(blockType);
        return byBlockType.stream()
                .map(dataBodyEntity -> modelMapper.map(dataBodyEntity, DataEnvelope.class))
                .collect(Collectors.toList());
    }

    @Override
    public boolean updateDataBlockType(String blockName, BlockTypeEnum blockType) {
        Optional<DataBodyEntity> dataByBlockName = dataBodyService.getDataByBlockName(blockName);

        if (dataByBlockName.isPresent()) {
            DataBodyEntity dataBodyEntity = dataByBlockName.get();
            dataBodyEntity.getDataHeaderEntity().setBlocktype(blockType);
            dataBodyService.saveDataBody(dataBodyEntity);
            return true;
        }
        return false;
    }
}
