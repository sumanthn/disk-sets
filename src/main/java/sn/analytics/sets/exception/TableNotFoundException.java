package sn.analytics.sets.exception;

/**
 * Created by sumanth
 */
public class TableNotFoundException extends Exception {
    public TableNotFoundException(String message) {
        super(message);
    }

    public TableNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
