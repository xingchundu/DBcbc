package org.dbcbc.connector.dm;

public class DmException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DmException(String message) {
        super(message);
    }

    public DmException(String message, Throwable cause) {
        super(message, cause);
    }

    public DmException(Throwable cause) {
        super(cause);
    }
}
