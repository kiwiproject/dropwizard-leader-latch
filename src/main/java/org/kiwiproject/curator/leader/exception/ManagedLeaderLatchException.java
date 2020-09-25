package org.kiwiproject.curator.leader.exception;

/**
 * Exception that is thrown when there is some unexpected error relating to a managed Curator leader latch.
 */
public class ManagedLeaderLatchException extends RuntimeException {

    public ManagedLeaderLatchException() {
    }

    public ManagedLeaderLatchException(String message) {
        super(message);
    }

    public ManagedLeaderLatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public ManagedLeaderLatchException(Throwable cause) {
        super(cause);
    }
}
