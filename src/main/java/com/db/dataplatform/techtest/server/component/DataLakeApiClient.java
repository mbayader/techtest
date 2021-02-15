package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;

public interface DataLakeApiClient {
    void save(DataEnvelope envelope);
}
