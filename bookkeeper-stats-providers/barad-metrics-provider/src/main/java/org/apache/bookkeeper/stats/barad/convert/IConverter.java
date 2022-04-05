package org.apache.bookkeeper.stats.barad.convert;

import java.util.Collections;
import java.util.List;

public interface IConverter<N, S, T> {

    T convert(N name, S source);

    boolean canConvert(String name);

    default List<T> multiConvert(N name, S source) {
        T result = convert(name, source);
        if (result != null) {
            return Collections.singletonList(convert(name, source));
        } else {
            return null;
        }
    }
}