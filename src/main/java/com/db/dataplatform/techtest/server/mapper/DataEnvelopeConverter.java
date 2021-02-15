package com.db.dataplatform.techtest.server.mapper;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import org.modelmapper.AbstractConverter;
import org.springframework.stereotype.Component;

@Component
public class DataEnvelopeConverter extends AbstractConverter<DataBodyEntity, DataEnvelope> {

    @Override
    protected DataEnvelope convert(DataBodyEntity source) {
        DataHeaderEntity dataHeaderEntity = source.getDataHeaderEntity();
        DataHeader dataHeader = new DataHeader(dataHeaderEntity.getName(), dataHeaderEntity.getBlocktype());
        DataBody dataBody = new DataBody(source.getDataBody());
        return new DataEnvelope(dataHeader, dataBody);
    }
}