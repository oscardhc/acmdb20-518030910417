package simpledb;

import java.util.function.Function;

public class U {

    static <T, K> T with(T in, Function<T, K> f) {
        f.apply(in);
        return in;
    }

}
