package sn.analytics.sets.exception;

/**
 * Created by sumanth
 */
public class SetExistsException extends Exception {

    public SetExistsException(String message) {
        super(message);
    }

    public SetExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
