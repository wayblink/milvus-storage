package io.milvus.storage.format.parquet;

import io.milvus.storage.format.Reader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class TestParquetFileReader {

    @Test
    public void testReadFile() throws Exception {
        String uri = "file:" + System.getProperty("user.dir") +  "/data/example.parquet";
        Reader reader = new ParquetFileReader(uri, 100);
        VectorSchemaRoot root = reader.Read();
        System.out.println(root.contentToTSVString());
    }

}
