package io.milvus.storage.storage.space;

import io.milvus.storage.common.constant.Constant;
import io.milvus.storage.storage.schema.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SpaceOptions {
    private Long version;
    private Schema schema;

    public SpaceOptions() {
        this(Constant.LatestManifestVersion);
    }

    public SpaceOptions(Long version) {
        this.version = version;
    }

}
