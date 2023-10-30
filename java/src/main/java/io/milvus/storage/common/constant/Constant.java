package io.milvus.storage.common.constant;

public class Constant {
    public final static Long ReadBatchSize          = 1024L;
    public final static String ManifestTempFileSuffix = ".manifest.tmp";
    public final static String ManifestFileSuffix     = ".manifest";
    public final static String ManifestDir            = "versions";
    public final static String BlobDir                = "blobs";
    public final static String ParquetDataFileSuffix  = ".parquet";
    public final static String OffsetFieldName        = "__offset";
    public final static String VectorDataDir          = "vector";
    public final static String ScalarDataDir          = "scalar";
    public final static String DeleteDataDir          = "delete";
    public final static Long LatestManifestVersion  = -1L;
    public final static String EndpointOverride = "endpoint_override";
}
