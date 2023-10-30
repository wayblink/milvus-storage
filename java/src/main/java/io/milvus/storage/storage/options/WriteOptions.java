package io.milvus.storage.storage.options;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class WriteOptions {
    private Long maxRecordPerFile;
}
