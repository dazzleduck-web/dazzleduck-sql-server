package io.dazzleduck.sql.http.server;

import io.helidon.http.HeaderNames;
import io.helidon.webserver.http.HttpRequest;

import java.util.function.Function;

public interface ParameterUtils {

    static <T> T getParameterValue(String parameter, HttpRequest request, T defaultValue, Class<T> tClass) {
        Function<String, T> mapFunction = null;
        if (tClass.equals(Long.class)) {
            mapFunction = s -> (T) Long.valueOf(Long.parseLong(s));
        } else if (tClass.equals(String.class)) {
            mapFunction = s -> (T) s;
        } else if (tClass.equals(Integer.class)) {
            mapFunction = s -> (T) Integer.valueOf(Integer.parseInt(s));
        } else if (tClass.equals(Boolean.class)) {
            mapFunction = s -> (T) Boolean.valueOf(Boolean.parseBoolean(s));
        }
        var fromParameter = request.query().first(parameter);
        var fromHeader = request.headers().value(HeaderNames.create(parameter));
        if (fromParameter.isPresent()) {
            return mapFunction.apply(fromParameter.get());
        }

        if (fromHeader.isPresent()) {
            return mapFunction.apply(fromHeader.get());
        }
        return defaultValue;
    }
}
