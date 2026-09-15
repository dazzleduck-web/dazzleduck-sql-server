package io.dazzleduck.sql.flight.server;

public class NoSuchCatalogSchemaError extends Exception {
    private final String catalogAndSchema;
    public NoSuchCatalogSchemaError(String catalogAndSchema) {
        super(String.format("Catalog or Schema %s Not Fount", catalogAndSchema));
        this.catalogAndSchema = catalogAndSchema;
    }

    /**
     * The connection setup batch carries more than the USE — a failed SET VARIABLE surfaces here
     * too. Keep the real cause so the logs say which statement actually failed.
     */
    public NoSuchCatalogSchemaError(String catalogAndSchema, Throwable cause) {
        super(String.format("Catalog or Schema %s Not Fount", catalogAndSchema), cause);
        this.catalogAndSchema = catalogAndSchema;
    }
}
