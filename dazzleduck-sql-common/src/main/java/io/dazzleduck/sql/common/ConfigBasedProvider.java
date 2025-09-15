package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

public interface ConfigBasedProvider {

    public static final String CLASS_KEY = "class";
    static <T> T load(Config config, String prefixKey, T defaultObject) throws Exception {
        if( config.hasPath(prefixKey)) {
            var innerConfig = config.getConfig(prefixKey);
            var clazz =  innerConfig.getString(CLASS_KEY);
            var constructor = Class.forName(clazz).getConstructor();
            @SuppressWarnings("unchecked")
            T object = (T) constructor.newInstance();
            var loadMethod = Class.forName(clazz).getMethod("setConfig", Config.class);
            loadMethod.invoke(object, innerConfig);
            return object;
        } else {
            return defaultObject;
        }
    }
}
