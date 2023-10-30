package io.milvus.storage.storage.options;

import io.milvus.storage.common.constant.Constant;
import io.milvus.storage.filter.Filter;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class ReadOptions {
    //Filters map[string]filter.Filter
    private List<Filter> filters;
    private List<String> columns;
    private Long manifestVersion;
    private Long version;
    private Long batchSize;

    private ReadOptions(List<Filter> filters, List<String> columns, Long manifestVersion, Long version, Long batchSize) {
        this.filters = filters;
        this.columns = columns;
        this.manifestVersion = manifestVersion;
        this.version = version;
        this.batchSize = batchSize;
    }

    public static final class Builder {
        private List<Filter> filters = new ArrayList<>();
        private List<String> columns = new ArrayList<>();
        private Long manifestVersion = Constant.LatestManifestVersion;
        private Long version = Long.MAX_VALUE;
        private Long batchSize = 1L;

        public Builder setFilters(List<Filter> filters) {
            this.filters = filters;
            return this;
        }

        public Builder setColumns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setManifestVersion(Long manifestVersion) {
            this.manifestVersion = manifestVersion;
            return this;
        }

        public Builder setVersion(Long version) {
            this.version = version;
            return this;
        }

        public Builder setBatchSize(Long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(filters, columns, manifestVersion, version, batchSize);
        }
    }
}
