package io.milvus.storage.storage.transaction;

import io.milvus.storage.common.exception.FieldNotFoundException;
import io.milvus.storage.common.exception.SchemaNotMatchException;

import java.io.IOException;

public interface Operation {
    void Execute() throws SchemaNotMatchException, FieldNotFoundException, IOException;
}
