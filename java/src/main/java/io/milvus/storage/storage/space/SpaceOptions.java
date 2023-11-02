package io.milvus.storage.storage.space;

import io.milvus.storage.common.constant.Constant;
import io.milvus.storage.filter.Filter;
import io.milvus.storage.storage.options.ReadOptions;
import io.milvus.storage.storage.schema.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@Getter
public class SpaceOptions {
    private Long version;
    @NonNull
    private Schema schema;

    private SpaceOptions(Long version, Schema schema) {
        this.version = version;
        this.schema = schema;
    }

    public static final class Builder {
        private Long version = Constant.LatestManifestVersion;
        private Schema schema;

        public SpaceOptions.Builder setVersion(Long version) {
            this.version = version;
            return this;
        }

        public SpaceOptions.Builder setSchema(Schema schema) {
            this.schema = schema;
            return this;
        }
        public SpaceOptions build() {
            return new SpaceOptions(version, schema);
        }

    }
}
