package io.milvus.storage.storage;

import io.milvus.storage.file.fragment.DeleteFragmentVector;
import io.milvus.storage.storage.manifest.Manifest;
import lombok.Getter;

@Getter
public class Space {
    //        fs              fs.Fs
    private String path;
    private DeleteFragmentVector deleteFragments;
    private Manifest manifest;
    //        lockManager     lock.LockManager

    public Space() {
    }

    public Space(String path, DeleteFragmentVector deleteFragments, Manifest manifest) {
        this.path = path;
        this.deleteFragments = deleteFragments;
        this.manifest = manifest;
    }

    public void Write(/*reader array.RecordReader, options *options.WriteOptions*/) {

    }

    public void Delete(/* reader array.RecordReader */) {

    }

    // Open opened a space or create if the space does not exist.
    // If space does not exist. schema should not be nullptr, or an error will be returned.
    // If space exists and version is specified, it will restore to the state at this version,
    // or it will choose the latest version.
    public static Space Open(String uri) {
        return new Space();
    }

    //todo other methods



}
