package io.milvus.storage.storage.transaction;

import io.milvus.storage.common.constant.Constant;
import io.milvus.storage.common.exception.FieldNotFoundException;
import io.milvus.storage.common.exception.SchemaNotMatchException;
import io.milvus.storage.file.fragment.Fragment;
import io.milvus.storage.format.Writer;
import io.milvus.storage.format.parquet.ParquetFileWriter;
import io.milvus.storage.storage.manifest.ManifestReaderWriter;
import io.milvus.storage.storage.options.WriteOptions;
import io.milvus.storage.storage.space.Space;
import io.milvus.storage.storage.utils.StorageUtils;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WriteOperation implements Operation{
    private VectorSchemaRoot data;
    private WriteOptions options;
    private Space space;

    private Writer scalarWriter;
    private Writer vectorWriter;

    public WriteOperation(VectorSchemaRoot data, WriteOptions options, Space space) {
        this.data = data;
        this.options = options;
        this.space = space;
    }

    @Override
    public void Execute() throws SchemaNotMatchException, FieldNotFoundException, IOException {
        if (!space.getManifest().getSchema().getSchema().equals(data.getSchema())) {
            throw new SchemaNotMatchException(space.getManifest().getSchema().getSchema().toString(), data.getSchema().toString());
        }

        Schema scalarSchema = space.getManifest().getSchema().getScalarSchema();
        Schema vectorSchema = space.getManifest().getSchema().getVectorSchema();

        Fragment scalarFragment = new Fragment();
        Fragment vectorFragment = new Fragment();

//        VectorSchemaRoot scalarData = WriteOperation.projection(data, scalarSchema, true);
//        VectorSchemaRoot vectorData = WriteOperation.projection(data, vectorSchema, true);

        String scalarDataPath = StorageUtils.GetNewParquetFilePath(StorageUtils.GetScalarDataDir(this.space.getPath()));
        String vectorDataPath = StorageUtils.GetNewParquetFilePath(StorageUtils.GetVectorDataDir(this.space.getPath()));

        // todo file split when larger than maxRecordPerFile
        scalarWriter = new ParquetFileWriter(scalarDataPath, scalarSchema);
        vectorWriter = new ParquetFileWriter(vectorDataPath, vectorSchema);

        vectorWriter.Write(data);
        scalarWriter.Write(data);

        scalarFragment.AddFile(scalarDataPath);
        vectorFragment.AddFile(vectorDataPath);

        scalarWriter.Close();
        vectorWriter.Close();

        // commit change
        space.getManifest().AddScalarFragment(scalarFragment);
        space.getManifest().AddVectorFragment(vectorFragment);

        ManifestReaderWriter rw = new ManifestReaderWriter(space.getFs(), space.getPath());
        rw.Write(space.getManifest());


        return;
    }

//    private List<Fragment> write(Writer writer, Schema schema, VectorSchemaRoot data, boolean isScalar) {
//
//    }

    public static VectorSchemaRoot projection(VectorSchemaRoot source, Schema schema, boolean isScalar) throws FieldNotFoundException {
        List<FieldVector> selectedFields = new ArrayList();
        selectedFields.addAll(source.getFieldVectors());

        if (isScalar) {
            BigIntVector offsetVector = new BigIntVector(Constant.OffsetFieldName, source.getFieldVectors().get(0).getAllocator());
            offsetVector.allocateNew(source.getRowCount());
            for (int i = 0; i < source.getRowCount(); i++) {
                offsetVector.set(i, i);
            }
            offsetVector.setValueCount(source.getRowCount());
            selectedFields.add(offsetVector);
        }

//        for (Field field: schema.getFields()) {
//            FieldVector selectedField;
//            if (field.getName().equals(Constant.OffsetFieldName)) {
//                BigIntVector offsetVector = new BigIntVector(Constant.OffsetFieldName, source.getFieldVectors().get(0).getAllocator());
//                offsetVector.allocateNew(source.getRowCount());
//                for (int i = 0; i < source.getRowCount(); i++) {
//                    offsetVector.set(i, i);
//                }
//                offsetVector.setValueCount(source.getRowCount());
//                selectedFields.add(offsetVector);
//            } else {
//                selectedField = source.getVector(field.getName());
//                if (selectedField != null) {
//                    selectedFields.add(selectedField);
//                } else {
//                    throw new FieldNotFoundException(field.getName());
//                }
//            }
//        }
        return new VectorSchemaRoot(schema, selectedFields, source.getRowCount());
    }
}
