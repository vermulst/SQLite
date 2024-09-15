package me.vermulst.vermulstutils.data;

import java.util.List;
import java.util.Objects;

public class CompositeKey {

    private final List<Object> keyParts;

    public CompositeKey(List<Object> keyParts) {
        this.keyParts = keyParts;
    }

    public List<Object> getKeyParts() {
        return keyParts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(keyParts, that.keyParts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyParts);
    }

    @Override
    public String toString() {
        return keyParts.toString();
    }


    public static CompositeKey of(Object... keys) {
        return new CompositeKey(List.of(keys));
    }
}
