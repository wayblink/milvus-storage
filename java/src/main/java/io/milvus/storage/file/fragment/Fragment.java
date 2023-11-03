package io.milvus.storage.file.fragment;

import lombok.*;
import manifest_proto.ManifestOuterClass;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
public class Fragment {
    private Long id = 0L;
    private List<String> files;

    public Fragment() {
        this.files = new ArrayList<>();
    }

    public Fragment(Long id) {
        this(id, new ArrayList<>());
    }

    public Fragment(Long id, List<String> files) {
        this.id = id;
        this.files = files;
    }

    public static List<String> ToFilesVector(List<Fragment> fragments) {
        List<String> files = new ArrayList<>();
        fragments.forEach(f -> files.addAll(f.files));
        return files;
    }

    public void AddFile(String file) {
        this.files.add(file);
    }

    public static Fragment FromProtobuf(ManifestOuterClass.Fragment fragment) {
        return new Fragment(fragment.getId(), fragment.getFilesList());
    }

    public ManifestOuterClass.Fragment ToProtobuf() {
        return ManifestOuterClass.Fragment.newBuilder()
                .setId(this.getId())
                .addAllFiles(this.getFiles())
                .build();
    }
}

