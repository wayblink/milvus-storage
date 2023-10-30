package io.milvus.storage.format.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Row;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;

public class TestParquetFileWriter {

    @Test
    public void testWriteParquet() throws Exception {
        // schema
        Field a = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), /*children*/ null);
        Field b = new Field("name", FieldType.nullable(new ArrowType.Utf8()), /*children*/ null);
        Map<String, String> metadata = new HashMap<>();
        Schema arrowSchema = new Schema(asList(a, b), metadata);

        String path = "file:" + System.getProperty("user.dir") + "/output.parquet";
        ParquetFileWriter writer = new ParquetFileWriter(path, arrowSchema);

        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        IntVector ageVector = (IntVector) root.getVector("age");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        root.setRowCount(3);
        ageVector.allocateNew(3);
        ageVector.set(0, 10);
        ageVector.set(1, 20);
        ageVector.set(2, 30);
        nameVector.allocateNew(3);
        nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
        nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
        nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
        System.out.println("VectorSchemaRoot created: \n" + root.contentToTSVString());

        writer.Write(root);
        writer.Close();
    }
}
