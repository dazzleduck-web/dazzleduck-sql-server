package io.dazzleduck.sql.logger;

import org.slf4j.spi.MDCAdapter;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;

/**
 * MDC adapter that stores context data in thread-local storage
 */
public class ArrowMDCAdapter implements MDCAdapter {

    private final ThreadLocal<Map<String, String>> contextMap = ThreadLocal.withInitial(HashMap::new);
    private final ThreadLocal<Deque<Map<String, String>>> contextStack = ThreadLocal.withInitial(LinkedList::new);

    @Override
    public void put(String key, String val) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        contextMap.get().put(key, val);
    }

    @Override
    public String get(String key) {
        return contextMap.get().get(key);
    }

    @Override
    public void remove(String key) {
        contextMap.get().remove(key);
    }

    @Override
    public void clear() {
        contextMap.get().clear();
    }

    @Override
    public Map<String, String> getCopyOfContextMap() {
        Map<String, String> map = contextMap.get();
        return map.isEmpty() ? null : new HashMap<>(map);
    }

    @Override
    public void setContextMap(Map<String, String> contextMap) {
        this.contextMap.get().clear();
        if (contextMap != null) {
            this.contextMap.get().putAll(contextMap);
        }
    }

    @Override
    public void pushByKey(String key, String value) {
        Deque<Map<String, String>> stack = contextStack.get();
        Map<String, String> current = new HashMap<>(contextMap.get());
        stack.push(current);
        put(key, value);
    }

    @Override
    public String popByKey(String key) {
        Deque<Map<String, String>> stack = contextStack.get();
        if (stack.isEmpty()) {
            return null;
        }

        Map<String, String> previous = stack.pop();
        String value = get(key);
        contextMap.set(previous);
        return value;
    }

    @Override
    public Deque<String> getCopyOfDequeByKey(String key) {
        // This is a simplified implementation
        Deque<String> deque = new LinkedList<>();
        String value = get(key);
        if (value != null) {
            deque.push(value);
        }
        return deque;
    }

    @Override
    public void clearDequeByKey(String key) {
        remove(key);
    }
}