package io.milvus.storage.storage.space;

import io.milvus.storage.file.fragment.DeleteFragmentVector;
import io.milvus.storage.storage.manifest.Manifest;
import io.milvus.storage.storage.manifest.ManifestReaderWriter;
import io.milvus.storage.storage.transaction.WriteOperation;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

@Getter
public class Space {
    //        fs              fs.Fs
    private String path;
    private DeleteFragmentVector deleteFragments;
    private Manifest manifest;
    // todo support transaction in the future
    // private lock.LockManager lockManager;

    public Space() {
    }

    public Space(String path, Manifest manifest) {
        this.path = path;
        this.deleteFragments = new DeleteFragmentVector();
        this.manifest = manifest;
    }

    public Space(String path, DeleteFragmentVector deleteFragments, Manifest manifest) {
        this.path = path;
        this.deleteFragments = deleteFragments;
        this.manifest = manifest;
    }

    public void Write(/*reader array.RecordReader, options *options.WriteOptions*/) {
        new WriteOperation().Execute();
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

        // todo fileSystem
        FileSystem fs = FileSystem.newInstance(new Configuration());

        Path path = new Path(uri);
        // create if not exist
        // 1, manifest
        // 2, scalar data
        // 3, vector data
        // 4, blob
        // 5, delete data

        ManifestReaderWriter manifestReaderWriter = new ManifestReaderWriter(fs, uri);

        Manifest m = null;
        try {
            m = manifestReaderWriter.Read(options.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
            // not exist, create the first manifest
            if (options.getSchema() == null) {
                throw new Exception("schema is nil");
            }
            m = new Manifest(options.getSchema());
            m.setVersion(0L);
            manifestReaderWriter.Write(m);
        }
        return new Space(uri, m);
    }

    //todo other methods



}
