package io.milvus.storage.file.fragment;

import io.milvus.storage.storage.manifest.Manifest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import manifest_proto.ManifestOuterClass;

import java.util.ArrayList;
import java.util.List;

@Getter
public class FragmentVector {
    List<Fragment> fragments;

    public FragmentVector() {
        this.fragments = new ArrayList<>();
    }

    public FragmentVector(List<Fragment> fragments) {
        this.fragments = fragments;
    }

    public void AddFragment(Fragment fragment) {
        this.fragments.add(fragment);
    }

    public List<ManifestOuterClass.Fragment> ToProtoBuf() {
        List<ManifestOuterClass.Fragment> list = new ArrayList<>();
        for (int i = 0; i < this.fragments.size(); i++) {
            list.add(this.fragments.get(i).ToProtobuf());
        }
        return list;
    }

    public static FragmentVector FromProtoBuf(List<ManifestOuterClass.Fragment> fragments) {
        List<Fragment> list = new ArrayList<>();
        for (int i = 0; i < fragments.size(); i++) {
            list.add(Fragment.FromProtobuf(fragments.get(i)));
        }
        return new FragmentVector(list);
    }
}
