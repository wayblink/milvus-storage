package io.milvus.storage.storage.space;

import io.milvus.storage.common.exception.FieldNotFoundException;
import io.milvus.storage.common.exception.SchemaNotMatchException;
import io.milvus.storage.file.fragment.DeleteFragmentVector;
import io.milvus.storage.fs.FileSystem;
import io.milvus.storage.fs.FsFactory;
import io.milvus.storage.storage.manifest.Manifest;
import io.milvus.storage.storage.manifest.ManifestReaderWriter;
import io.milvus.storage.storage.options.WriteOptions;
import io.milvus.storage.storage.transaction.WriteOperation;
import lombok.Getter;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

@Getter
public class Space {
    private FileSystem fs;
    private String path;
    private DeleteFragmentVector deleteFragments;
    private Manifest manifest;
    // todo support transaction in the future
    // private lock.LockManager lockManager;

    public Space() {
    }

    public Space(FileSystem fs, String path, Manifest manifest) {
        this(fs, path, new DeleteFragmentVector(), manifest);
    }

    public Space(FileSystem fs, String path, DeleteFragmentVector deleteFragments, Manifest manifest) {
        this.fs = fs;
        this.path = path;
        this.deleteFragments = deleteFragments;
        this.manifest = manifest;
    }

    public void Write(VectorSchemaRoot data, WriteOptions options) throws SchemaNotMatchException, FieldNotFoundException, IOException {
        new WriteOperation(data, options, this).Execute();
    }

    public void Delete(/* reader array.RecordReader */) {

    }

    public void WriteBlob(Byte[] content, String name, Boolean replace) {

    }

    // Open opened a space or create if the space does not exist.
    // If space does not exist. schema should not be nullptr, or an error will be returned.
    // If space exists and version is specified, it will restore to the state at this version,
    // or it will choose the latest version.
    public static Space Open(String uri, SpaceOptions options) throws Exception {
        FileSystem fs = FsFactory.Create(uri);

        // create if not exist
        // 1, manifest
        // 2, scalar data
        // 3, vector data
        // 4, blob
        // 5, delete data

        ManifestReaderWriter manifestReaderWriter = new ManifestReaderWriter(fs, uri);

        Manifest m = manifestReaderWriter.Read(options.getVersion());
        if (m == null) {
            m = new Manifest(options.getSchema());
            m.setVersion(0L);
            manifestReaderWriter.Write(m);
        }
        if (options.getSchema() == null) {
            throw new Exception("schema is nil");
        }
        return new Space(fs, uri, m);
    }

    //todo other methods



}
