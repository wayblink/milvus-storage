package io.milvus.storage.file.blob;

import lombok.Getter;
import lombok.ToString;
import manifest_proto.ManifestOuterClass;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
public class Blob {

    private String name;
    private Long size;
    private String file;

    public Blob(String name, Long size, String file) {
        this.name = name;
        this.size = size;
        this.file = file;
    }

    public ManifestOuterClass.Blob ToProtobuf() {
        return ManifestOuterClass.Blob.newBuilder()
                .setName(this.getName())
                .setSize(this.getSize())
                .setFile(this.getFile())
                .build();
    }

    public static Blob FromProtobuf(ManifestOuterClass.Blob blob) {
        return new Blob(blob.getName(), blob.getSize(), blob.getFile());
    }

    public static List<ManifestOuterClass.Blob> ToProtoBuf(List<Blob> blobs) {
        List<ManifestOuterClass.Blob> list = new ArrayList<>();
        for (int i = 0; i < blobs.size(); i++) {
            list.add(blobs.get(i).ToProtobuf());
        }
        return list;
    }

    public static List<Blob> FromProtobuf(List<ManifestOuterClass.Blob> blobs) {
        List<Blob> list = new ArrayList<>();
        for (int i = 0; i < blobs.size(); i++) {
            list.add(FromProtobuf(blobs.get(i)));
        }
        return list;
    }
}
