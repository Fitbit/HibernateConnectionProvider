package com.fitbit.hibernate.connection.event;

import java.sql.Connection;

/**
 * Listener type that listens only to post-connection release (close) events, which includes both successfully
 * relinquishing a connection as well as any failures to do so.
 *
 * @author dgarson
 */
public interface PostConnectionCloseListener extends ConnectionProviderListener {

    /**
     * Callback invoked immediately after successfully releasing a connection back to the connection pool. This callback
     * method does not have access to the {@link Connection} instance itself since it is an undefined state once it is
     * returned to the pool.
     */
    void afterConnectionClosed();

    /**
     * Callback invoked when an exception occurs while trying to release a connection back to the pool.
     *
     * @param connection the connection that we tried to release
     * @param exc the exception that was thrown
     */
    void afterConnectionClosingFailed(Connection connection, Throwable exc);
}

