package io.dazzleduck.sql.http.server;

/**
 * Configuration for HTTP query services.
 * Provides default values and builder pattern for customization.
 */
public class HttpConfig {

    private final long queryTimeoutMs;

    private HttpConfig(Builder builder) {
        this.queryTimeoutMs = builder.queryTimeoutMs;
    }

    public long getQueryTimeoutMs() {
        return queryTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static HttpConfig defaultConfig() {
        return builder().build();
    }

    public static class Builder {
        private long queryTimeoutMs = 120000; // Default 120 seconds (2 minutes)

        private Builder() {
        }

        /**
         * Set the query execution timeout in milliseconds.
         * @param queryTimeoutMs timeout in milliseconds (must be positive)
         * @return this builder
         */
        public Builder queryTimeoutMs(long queryTimeoutMs) {
            if (queryTimeoutMs <= 0) {
                throw new IllegalArgumentException("queryTimeoutMs must be positive, got: " + queryTimeoutMs);
            }
            this.queryTimeoutMs = queryTimeoutMs;
            return this;
        }

        public HttpConfig build() {
            return new HttpConfig(this);
        }
    }
}
