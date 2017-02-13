/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.postgresql;


import com.googlecode.cqengine.index.support.CloseableIterator;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public abstract class PostgreSQLStatementIterator<T> implements CloseableIterator<T> {

    public static abstract class Listener<T> {
        public void resultSetConsumed(ResultSet resultSet, T t) {}
        public void resultSetClosed() {}
    }

    protected ResultSet resultSet;
    protected final PreparedStatement statement;
    protected final Connection connection;

    private boolean nextCalled = true;

    @Getter @Setter
    public Listener<T> listener;

    @SneakyThrows
    public PostgreSQLStatementIterator(PreparedStatement statement, Connection connection, boolean lazy) {
        this.statement = statement;
        this.connection = connection;
        if (!lazy) {
            resultSet = statement.executeQuery();
        }
    }

    @SneakyThrows
    @Override
    public boolean hasNext() {
        // lazy query execution
        if (resultSet == null) {
            resultSet = statement.executeQuery();
        }

        // if the result set is already resultSetClosed,
        // there are no more results to expect
        if (resultSet.isClosed()) {
            return false;
        }

        if (nextCalled) {
            nextCalled = false;
            if (resultSet.next()) {
                return true;
            } else {
                close();
                return false;
            }
        } else {
            return true;
        }
    }

    @Override public T next() {
        nextCalled = true;
        T result = fetchNext();
        if (listener != null) {
            listener.resultSetConsumed(resultSet, result);
        }
        return result;
    }

    protected abstract T fetchNext();

    @SneakyThrows
    @Override
    public void close() {
        if (resultSet != null && !resultSet.isClosed()) resultSet.close();
        if (!statement.isClosed()) statement.close();

        if (listener != null) {
            listener.resultSetClosed();
        }

        if (!connection.isClosed()) connection.close();
    }
}
