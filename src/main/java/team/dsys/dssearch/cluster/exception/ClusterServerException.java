package team.dsys.dssearch.cluster.exception;

/**
 * Cluster server exception(can add more types)
 */
public class ClusterServerException extends RuntimeException {

    public ClusterServerException() {
        super();
    }

    public ClusterServerException(String message) {
        super(message);
    }

    public ClusterServerException(String message, Throwable cause) {
        super(message, cause);
    }

}