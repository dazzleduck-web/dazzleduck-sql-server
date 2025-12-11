package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

public interface ConfigBasedProvider {

    String CLASS_KEY = "class";
    Class<?>[] constructorParameterTypes = {Config.class};

    static  <T extends  ConfigBasedProvider > T load(Config config, String prefixKey, T defaultObject) throws Exception {
        if (!config.hasPath(prefixKey)) {
            return defaultObject;
        }
        var innerConfig = config.getConfig(prefixKey);
        if (!innerConfig.hasPath(CLASS_KEY)) {
            return defaultObject;
        }
        var clazz = innerConfig.getString(CLASS_KEY);
        var c =  Class.forName(clazz);
        try {
            var constructorWithConfig = c.getConstructor(constructorParameterTypes);
            return (T) constructorWithConfig.newInstance(innerConfig);
        }  catch (NoSuchMethodException e) {
            var constructor = Class.forName(clazz).getConstructor();
            var object = (T) constructor.newInstance();
            object.setConfig(innerConfig);
            return object;
        }

    }

    void setConfig(Config config);

    private static <T extends ConfigBasedProvider>  T returnDefault(T defaultObject,  Config config) {
        defaultObject.setConfig(config);
        return defaultObject;
    }
}
