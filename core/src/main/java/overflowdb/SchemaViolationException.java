package overflowdb;

public class SchemaViolationException extends RuntimeException {
    public SchemaViolationException(String message) {
        super(message);
    }

    public SchemaViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
