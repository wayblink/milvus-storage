package io.milvus.storage.storage.manifest;

import io.milvus.storage.file.blob.Blob;
import io.milvus.storage.file.fragment.Fragment;
import io.milvus.storage.file.fragment.FragmentVector;
import io.milvus.storage.storage.schema.Schema;
import io.milvus.storage.storage.schema.SchemaException;
import io.milvus.storage.storage.schema.SchemaOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import manifest_proto.ManifestOuterClass;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class Manifest {
    private Long version;
    private Schema schema;
    private volatile FragmentVector scalarFragments;
    private volatile FragmentVector vectorFragments;
    private volatile FragmentVector deleteFragments;
    private volatile List<Blob> blobs;

    public Manifest(Schema schema) {
        this.schema = schema;
        this.scalarFragments = new FragmentVector();
        this.vectorFragments = new FragmentVector();
        this.deleteFragments = new FragmentVector();
        this.blobs = new ArrayList<>();
    }

    public Manifest() throws SchemaException {
        SchemaOptions schemaOptions = SchemaOptions.DefaultSchemaOptions();
        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
                new org.apache.arrow.vector.types.pojo.Schema(new ArrayList<Field>()/*, schemaOptions*/);
        this.schema = new Schema(arrowSchema, schemaOptions);
        this.scalarFragments = new FragmentVector();
        this.vectorFragments = new FragmentVector();
        this.deleteFragments = new FragmentVector();
        this.blobs = new ArrayList<>();
    }

    public void AddScalarFragment(Fragment fragment) {
        this.scalarFragments.AddFragment(fragment);
    }

    public void AddVectorFragment(Fragment fragment) {
        this.vectorFragments.AddFragment(fragment);
    }

    public void AddDeleteFragment(Fragment fragment) {
        this.deleteFragments.AddFragment(fragment);
    }

    public Boolean HasBlob(String name) {
        for (int i = 0; i < this.blobs.size(); i++) {
            if (this.blobs.get(i).getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    public Blob GetBlob(String name) {
        for (int i = 0; i < this.blobs.size(); i++) {
            if (this.blobs.get(i).getName().equals(name)) {
                return this.blobs.get(i);
            }
        }
        return null;
    }

    public void AddBlob(Blob blob) {
        this.blobs.add(blob);
    }

    public void RemoveBlobIfExist(String name) {
        for (int i = 0; i < this.blobs.size(); i++) {
            if (this.blobs.get(i).getName().equals(name)) {
                this.blobs.remove(i);
                return;
            }
        }
        return;
    }

    public ManifestOuterClass.Manifest ToProtobuf() {
        ManifestOuterClass.Manifest.Builder builder = ManifestOuterClass.Manifest.newBuilder();
        builder.setSchema(this.schema.ToProtobuf());
        builder.addAllScalarFragments(this.scalarFragments.ToProtoBuf());
        builder.addAllVectorFragments(this.vectorFragments.ToProtoBuf());
        builder.addAllDeleteFragments(this.deleteFragments.ToProtoBuf());
        builder.addAllBlobs(Blob.ToProtoBuf(this.blobs));
        builder.setVersion(this.version);
        return builder.build();
    }

    public static Manifest FromProtobuf(ManifestOuterClass.Manifest manifest) throws Exception {
        return new Manifest(
                manifest.getVersion(),
                Schema.FromProtobuf(manifest.getSchema()),
                FragmentVector.FromProtoBuf(manifest.getScalarFragmentsList()),
                FragmentVector.FromProtoBuf(manifest.getVectorFragmentsList()),
                FragmentVector.FromProtoBuf(manifest.getDeleteFragmentsList()),
                Blob.FromProtobuf(manifest.getBlobsList()));
    }

    // func ParseFromFile(f fs.Fs, path string) (*Manifest, error) {
    //	manifest := Init()
    //	manifestProto := &manifest_proto.Manifest{}
    //
    //	buf, err := f.ReadFile(path)
    //	if err != nil {
    //		return nil, err
    //	}
    //	err = proto.Unmarshal(buf, manifestProto)
    //	if err != nil {
    //		log.Error("Failed to unmarshal manifest proto", log.String("err", err.Error()))
    //		return nil, fmt.Errorf("parse from file: %w", err)
    //	}
    //	manifest.FromProtobuf(manifestProto)
    //
    //	return manifest, nil
    //}

}
