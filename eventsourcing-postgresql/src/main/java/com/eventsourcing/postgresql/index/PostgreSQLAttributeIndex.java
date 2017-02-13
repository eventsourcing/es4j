/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.postgresql.index;

import com.eventsourcing.Entity;
import com.eventsourcing.EntityHandle;
import com.eventsourcing.ResolvedEntityHandle;
import com.eventsourcing.index.AbstractAttributeIndex;
import com.eventsourcing.index.*;
import com.eventsourcing.layout.Layout;
import com.eventsourcing.layout.SerializableComparable;
import com.eventsourcing.layout.TypeHandler;
import com.eventsourcing.postgresql.PostgreSQLSerialization;
import com.eventsourcing.postgresql.PostgreSQLStatementIterator;
import com.eventsourcing.queries.options.EagerFetching;
import com.eventsourcing.queries.options.NotSeenBy;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.support.*;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.persistence.support.ObjectSet;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.Equal;
import com.googlecode.cqengine.query.simple.Has;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.closeable.CloseableResultSet;
import com.impossibl.postgres.jdbc.PGSQLIntegrityConstraintViolationException;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.eventsourcing.postgresql.PostgreSQLSerialization.getParameter;
import static com.eventsourcing.postgresql.PostgreSQLSerialization.setValue;

public abstract class PostgreSQLAttributeIndex<A, O extends Entity> extends AbstractAttributeIndex<A, O> {
    public static final int MAX_ADDITION_BATCH = 1000;
    private AdditionProcessor additionProcessor;
    protected KeyObjectStore<UUID, EntityHandle<O>> keyObjectStore;

    protected static <A, O extends Entity> Attribute<O, ?> serializableComparable(Attribute<O, A> attribute) {
        if (SerializableComparable.class.isAssignableFrom(attribute.getAttributeType())) {
            Class<?> type = SerializableComparable.getType(attribute.getAttributeType());
            @SuppressWarnings("unchecked")
            MultiValueAttribute newAttribute = new SerializableComparableAttribute<O, A>(attribute, type);
            return newAttribute;
        } else {
            return attribute;
        }
    }

    /**
     * Protected constructor, called by subclasses.
     *
     * @param attribute        The attribute on which the index will be built
     * @param supportedQueries The set of {@link Query} types which the subclass implementation supports
     */
    protected PostgreSQLAttributeIndex(Attribute<O, A> attribute,
                                       Set<Class<? extends Query>> supportedQueries) {
        super(attribute, supportedQueries);
    }

    protected abstract DataSource getDataSource();
    protected abstract Layout<O> getLayout();
    protected abstract String getTableName();
    protected abstract TypeHandler getAttributeTypeHandler();
    protected abstract boolean isUnique();

    @SneakyThrows
    public CloseableIterable<A> getDistinctKeys(QueryOptions queryOptions) {
        Connection connection = getDataSource().getConnection();
        PreparedStatement s = connection.prepareStatement("SELECT DISTINCT key FROM " + getTableName() + " ORDER BY key");
        return () -> new PostgreSQLStatementIterator<A>(s, connection, true) {
            @Override public A fetchNext() {
                return (A) PostgreSQLSerialization.getValue(resultSet, new AtomicInteger(1), getAttributeTypeHandler());
            }
        };
    }

    @SneakyThrows
    public Integer getCountForKey(A key, QueryOptions queryOptions) {
        try (Connection connection = getDataSource().getConnection()) {
            try (PreparedStatement s = connection.prepareStatement("SELECT COUNT(key) FROM " + getTableName() + " WHERE " +
                                                                           "key = ?")) {
                setValue(connection, s, 1, getQuantizedValue(key), getAttributeTypeHandler());
                try (java.sql.ResultSet resultSet = s.executeQuery()) {
                    resultSet.next();
                    return resultSet.getInt(1);
                }
            }
        }
    }

    @SneakyThrows
    public Integer getCountOfDistinctKeys(QueryOptions queryOptions) {
        try (Connection connection = getDataSource().getConnection()) {
            try (PreparedStatement s = connection.prepareStatement("SELECT COUNT(DISTINCT key) FROM " + getTableName())) {
                try (java.sql.ResultSet resultSet = s.executeQuery()) {
                    resultSet.next();
                    return resultSet.getInt(1);
                }
            }
        }
    }

    @SneakyThrows
    public CloseableIterable<KeyStatistics<A>> getStatisticsForDistinctKeys(QueryOptions queryOptions) {
        return getKeyStatisticsForDistinctKeys("ASC");
    }

    @SneakyThrows
    public CloseableIterable<KeyStatistics<A>> getStatisticsForDistinctKeysDescending(QueryOptions queryOptions) {
        return getKeyStatisticsForDistinctKeys("DESC");
    }


    protected CloseableIterable<KeyStatistics<A>> getKeyStatisticsForDistinctKeys(String order)
            throws SQLException {Connection connection = getDataSource().getConnection();
        PreparedStatement s = connection.prepareStatement("SELECT DISTINCT key, COUNT(key) FROM " + getTableName() + " " +
                                                                  "GROUP BY key ORDER BY key " + order);
        return new CloseableIterable<KeyStatistics<A>>() {
            @Override public CloseableIterator<KeyStatistics<A>> iterator() {
                return new PostgreSQLStatementIterator<KeyStatistics<A>>(s, connection, true) {
                    @SneakyThrows
                    @Override public KeyStatistics<A> fetchNext() {
                        A key = (A) PostgreSQLSerialization
                                .getValue(resultSet, new AtomicInteger(1), getAttributeTypeHandler());
                        int count = resultSet.getInt(2);
                        return new KeyStatistics<>(key, count);
                    }
                };
            }
        };
    }

    @SneakyThrows
    public CloseableIterable<KeyValue<A, EntityHandle<O>>> getKeysAndValues(QueryOptions queryOptions) {
        return queryKeysAndValues("ASC");
    }

    protected CloseableIterable<KeyValue<A, EntityHandle<O>>> queryKeysAndValues(String order)
            throws SQLException {Connection connection = getDataSource().getConnection();
        PreparedStatement s = connection
                .prepareStatement("SELECT key, value FROM " + getTableName() + " ORDER BY key " + order);
        return new CloseableIterable<KeyValue<A, EntityHandle<O>>>() {
            @Override public CloseableIterator<KeyValue<A, EntityHandle<O>>> iterator() {
                return new PostgreSQLStatementIterator<KeyValue<A, EntityHandle<O>>>(s, connection, true) {
                    @SneakyThrows
                    @Override public KeyValue<A, EntityHandle<O>> fetchNext() {
                        AtomicInteger i = new AtomicInteger(1);
                        A key = (A) PostgreSQLSerialization.getValue(resultSet, i, getAttributeTypeHandler());
                        UUID uuid = UUID.fromString(resultSet.getString(i.get()));
                        return new KeyValueMaterialized<>(key, keyObjectStore.get(uuid));
                    }
                };
            }
        };
    }

    @SneakyThrows
    public CloseableIterable<KeyValue<A, EntityHandle<O>>> getKeysAndValuesDescending(QueryOptions queryOptions) {
        return queryKeysAndValues("DESC");
    }


    @Override public boolean isMutable() {
        return true;
    }

    @Override public boolean isQuantized() {
        return false;
    }

    @Override public Index<EntityHandle<O>> getEffectiveIndex() {
        return this;
    }

    protected interface AdditionProcessor<O extends Entity, A> extends BiConsumer<EntityHandle<O>, A> {
        default void commit() throws SQLException {}
    }

    protected AdditionProcessor createAdditionProcessor() {
        return (AdditionProcessor<O, A>) (handle, attr) -> {};
    }

    @Override public boolean addAll(ObjectSet<EntityHandle<O>> objectSet, QueryOptions queryOptions) {
        try (CloseableIterator<EntityHandle<O>> iterator = objectSet.iterator()) {
            return addAll(iterator, queryOptions);
        }
    }

    @SneakyThrows
    public boolean addAll(Iterator<EntityHandle<O>> iterator, QueryOptions queryOptions) {
        try(Connection connection = getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            String insert = "INSERT INTO " + getTableName() + " VALUES (" + getParameter(connection, getAttributeTypeHandler(),
                                                                                    null) + ", ?::UUID) " +
                             (queryOptions.get(OnConflictDo.class) == null ? "" : "ON CONFLICT DO " + queryOptions.get
                             (OnConflictDo.class));
            while (iterator.hasNext()) {
                int counter = 0;

                try (PreparedStatement s = connection.prepareStatement(insert)) {
                    while (counter < MAX_ADDITION_BATCH && iterator.hasNext()) {
                        EntityHandle<O> object = iterator.next();
                        Iterator<A> attrIterator = getOwnAttribute().getValues(object, queryOptions).iterator();
                        while (attrIterator.hasNext()) {
                            int i = 1;
                            A attr = attrIterator.next();
                            i = setValue(connection, s, i, getQuantizedValue(attr), getAttributeTypeHandler());
                            s.setString(i, object.uuid().toString());
                            s.addBatch();
                            additionProcessor.accept(object, attr);
                            counter++;
                        }
                    }
                    try {
                        s.executeBatch();
                        additionProcessor.commit();
                    } catch (BatchUpdateException e) {
                        connection.rollback();
                        Throwable nextException = e.getCause();
                        if (nextException instanceof PGSQLIntegrityConstraintViolationException) {
                            if (nextException.getMessage().contains("duplicate key value violates unique constraint")) {
                                throw new UniqueIndex.UniqueConstraintViolatedException(nextException.getMessage());
                            } else {
                                throw e;
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }

            connection.commit();
        }

        return true;
    }

    protected com.googlecode.cqengine.attribute.Attribute<EntityHandle<O>, A> getOwnAttribute() {
        return attribute;
    }

    @SneakyThrows
    @Override public boolean removeAll(ObjectSet<EntityHandle<O>> objects, QueryOptions queryOptions) {
        try(Connection connection = getDataSource().getConnection()) {
            String insert = "DELETE FROM " + getTableName() + " WHERE object = ?::UUID";
            try (PreparedStatement s = connection.prepareStatement(insert)) {
                try (CloseableIterator<EntityHandle<O>> iterator = objects.iterator()) {
                    while (iterator.hasNext()) {
                        EntityHandle<O> object = iterator.next();
                        s.setString(1, object.uuid().toString());
                        s.addBatch();
                    }
                }
                s.executeBatch();
            }
        }

        return true;
    }

    @SneakyThrows
    @Override public void clear(QueryOptions queryOptions) {
        try(Connection connection = getDataSource().getConnection()) {
            try (PreparedStatement s = connection.prepareStatement("DELETE FROM " + getTableName())) {
                s.executeUpdate();
            }
        }

    }

    @Override public void init(ObjectStore<EntityHandle<O>> objectStore, QueryOptions queryOptions) {
        additionProcessor = createAdditionProcessor();
        if (objectStore instanceof KeyObjectStore) {
            this.keyObjectStore = (KeyObjectStore<UUID, EntityHandle<O>>) objectStore;
        } else {
            this.keyObjectStore = new SetKeyObjectStore(objectStore, queryOptions);
        }
        queryOptions.put(OnConflictDo.class, OnConflictDo.NOTHING);
        queryOptions.put(EagerFetching.class, true);
        queryOptions.put(NotSeenBy.class, new NotSeenBy(getTableName().getBytes()));
        addAll(objectStore.iterator(queryOptions), queryOptions);
    }

    @SneakyThrows
    public CloseableIterable<A> getDistinctKeys(A lowerBound, boolean lowerInclusive, A upperBound,
                                                boolean upperInclusive, QueryOptions queryOptions) {
        return queryDistinctKeys(lowerBound, lowerInclusive, upperBound, upperInclusive, "ASC");
    }

    protected CloseableIterable<A> queryDistinctKeys(A lowerBound, boolean lowerInclusive, A upperBound,
                                                     boolean upperInclusive, String order)
            throws SQLException {Connection connection = getDataSource().getConnection();
        String lowerOp = lowerInclusive ? ">=" : ">";
        String upperOp = upperInclusive ? "<=" : "<";
        String query = "SELECT DISTINCT key FROM " + getTableName() + " WHERE " +
                "key " + lowerOp + " ? AND " +
                "key " + upperOp + " ? " +
                "ORDER BY key " + order;
        PreparedStatement s = connection.prepareStatement(query);
        int i = setValue(connection, s, 1, lowerBound, getAttributeTypeHandler());
        setValue(connection, s, i, upperBound, getAttributeTypeHandler());
        return () -> new PostgreSQLStatementIterator<A>(s, connection, true) {
            @Override public A fetchNext() {
                return (A) PostgreSQLSerialization.getValue(resultSet, new AtomicInteger(1), getAttributeTypeHandler());
            }
        };
    }

    @SneakyThrows
    public CloseableIterable<A> getDistinctKeysDescending(QueryOptions queryOptions) {
        Connection connection = getDataSource().getConnection();
        PreparedStatement s = connection.prepareStatement("SELECT DISTINCT key FROM " + getTableName() + " ORDER BY " +
                                                                  "key DESC");
        return () -> new PostgreSQLStatementIterator<A>(s, connection, true) {
            @Override public A fetchNext() {
                return (A) PostgreSQLSerialization.getValue(resultSet, new AtomicInteger(1), getAttributeTypeHandler());
            }
        };
    }

    @SneakyThrows
    public CloseableIterable<A> getDistinctKeysDescending(A lowerBound, boolean lowerInclusive, A upperBound,
                                                          boolean upperInclusive, QueryOptions queryOptions) {
        return queryDistinctKeys(lowerBound, lowerInclusive, upperBound, upperInclusive, "DESC");
    }


    @SneakyThrows
    public CloseableIterable<KeyValue<A, EntityHandle<O>>> getKeysAndValues(A lowerBound, boolean lowerInclusive,
                                                                            A upperBound, boolean upperInclusive,
                                                                            QueryOptions queryOptions) {
        return queryKeysAndValues(lowerBound, lowerInclusive, upperBound, upperInclusive, queryOptions, "ASC");
    }


    @SneakyThrows
    public CloseableIterable<KeyValue<A, EntityHandle<O>>> getKeysAndValuesDescending(A lowerBound,
                                                                                      boolean lowerInclusive,
                                                                                      A upperBound,
                                                                                      boolean upperInclusive,
                                                                                      QueryOptions queryOptions) {
        return queryKeysAndValues(lowerBound, lowerInclusive, upperBound, upperInclusive, queryOptions, "DESC");
    }

    protected CloseableIterable<KeyValue<A, EntityHandle<O>>> queryKeysAndValues(A lowerBound, boolean lowerInclusive,
                                                                                 A upperBound, boolean upperInclusive,
                                                                                 QueryOptions queryOptionsString,
                                                                                 String order)
            throws SQLException {Connection connection = getDataSource().getConnection();
        String lowerOp = lowerInclusive ? ">=" : ">";
        String upperOp = upperInclusive ? "<=" : "<";
        String sql = "SELECT key, value FROM " + getTableName() +
                " WHERE " +
                "key " + lowerOp + " ? AND " +
                "key " + upperOp + " ? " +
                " ORDER BY key " + order;
        PreparedStatement s = connection.prepareStatement(sql);
        return new CloseableIterable<KeyValue<A, EntityHandle<O>>>() {
            @Override public CloseableIterator<KeyValue<A, EntityHandle<O>>> iterator() {
                return new PostgreSQLStatementIterator<KeyValue<A, EntityHandle<O>>>(s, connection, true) {
                    @SneakyThrows
                    @Override public KeyValue<A, EntityHandle<O>> fetchNext() {
                        AtomicInteger i = new AtomicInteger(1);
                        A key = (A) PostgreSQLSerialization.getValue(resultSet, i, getAttributeTypeHandler());
                        UUID uuid = UUID.fromString(resultSet.getString(i.get()));
                        return new KeyValueMaterialized<>(key, keyObjectStore.get(uuid));
                    }
                };
            }
        };
    }

    @SneakyThrows
    @Override public ResultSet<EntityHandle<O>> retrieve(Query<EntityHandle<O>> query, QueryOptions queryOptions) {
        Class<?> queryClass = query.getClass();
        if (queryClass.equals(Equal.class)) {
            final Equal<EntityHandle<O>, A> equal = (Equal<EntityHandle<O>, A>) query;
            Connection connection = getDataSource().getConnection();

            int size;
            A value = ((Equal<EntityHandle<O>, A>) query).getValue();

            try(PreparedStatement counter = connection
                    .prepareStatement("SELECT count(object) FROM " + getTableName() + " WHERE key = " + getParameter
                            (connection, getAttributeTypeHandler(), null))) {
                setValue(connection, counter, 1, value,
                         getAttributeTypeHandler());
                try (java.sql.ResultSet resultSet = counter.executeQuery()) {
                    resultSet.next();
                    size = resultSet.getInt(1);
                }
            }

            PreparedStatement s = connection
                    .prepareStatement("SELECT object FROM " + getTableName() + " WHERE key = " +
                                              getParameter(connection, getAttributeTypeHandler(), null));
            setValue(connection, s, 1, value, getAttributeTypeHandler());

            PostgreSQLStatementIterator<EntityHandle<O>> iterator = new PostgreSQLStatementIterator<EntityHandle<O>>
                    (s, connection, isMutable()) {
                @SneakyThrows
                @Override public EntityHandle<O> fetchNext() {
                    UUID uuid = UUID.fromString(resultSet.getString(1));
                    return keyObjectStore.get(uuid);
                }
            };


            int finalSize = size;
            ResultSet<EntityHandle<O>> rs = new MatchingResultSet<>(iterator, equal, queryOptions, finalSize);
            return new CloseableResultSet<>(rs, query, queryOptions);
        } else if (queryClass.equals(Has.class)) {
            final Has<EntityHandle<O>, A> has = (Has<EntityHandle<O>, A>) query;

            Connection connection = getDataSource().getConnection();

            int size;
            try (PreparedStatement counter = connection
                    .prepareStatement("SELECT count(object) FROM " + getTableName())) {
                try (java.sql.ResultSet resultSet = counter.executeQuery()) {
                    resultSet.next();
                    size = resultSet.getInt(1);
                }
            }

            PreparedStatement s = connection
                    .prepareStatement("SELECT object FROM " + getTableName());

            PostgreSQLStatementIterator<EntityHandle<O>> iterator = new PostgreSQLStatementIterator<EntityHandle<O>>(s,
                                                                                                                     connection,
                                                                                                                     isMutable()) {
                @SneakyThrows
                @Override public EntityHandle<O> fetchNext() {
                    UUID uuid = UUID.fromString(resultSet.getString(1));
                    return keyObjectStore.get(uuid);
                }
            };

            int finalSize = size;
            ResultSet<EntityHandle<O>> rs = new HasResultSet<>(iterator, has, queryOptions, finalSize);
            return new CloseableResultSet<>(rs, query, queryOptions);
        } else {
            throw new IllegalArgumentException("Unsupported query: " + query);
        }
    }

    protected static class SerializableComparableAttribute<O extends Entity, A> extends MultiValueAttribute<O, A> {

        @Getter
        private final Attribute<O, A> attribute;

        public SerializableComparableAttribute(Attribute<O, A> attribute, Class<?> type) {
            super(attribute.getEffectiveObjectType(), attribute.getObjectType(), (Class<A>) type,
                  attribute.getAttributeName());
            this.attribute = attribute;
        }

        @SuppressWarnings("unchecked")
        @Override public Iterable<Object> getValues(Entity object, QueryOptions queryOptions) {
            Iterable<A> iterable = attribute.getValues(new ResolvedEntityHandle(object), queryOptions);
            ArrayList values = new ArrayList<>();
            for (A value : iterable) {
                SerializableComparable value1 = (SerializableComparable) value;
                values.add(value1.getSerializableComparable());
            }
            return values;
        }

        @Override public int hashCode() {
            return attribute.hashCode();
        }

        @Override public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof SerializableComparableAttribute) {
                return attribute.equals(((SerializableComparableAttribute) o).attribute);
            }
            if (o instanceof Attribute) {
                return attribute.equals(o);
            }
            return false;
        }
    }

    class SetKeyObjectStore implements KeyObjectStore<UUID, EntityHandle<O>> {

        private final ObjectStore<EntityHandle<O>> objectStore;
        private final QueryOptions queryOptions;

        public SetKeyObjectStore(ObjectStore<EntityHandle<O>> objectStore, QueryOptions queryOptions) {
            this.objectStore = objectStore;
            this.queryOptions = queryOptions;
        }

        @Override public EntityHandle<O> get(UUID key) {
            CloseableIterator<EntityHandle<O>> iterator = objectStore.iterator(queryOptions);
            while (iterator.hasNext()) {
                EntityHandle<O> next = iterator.next();
                if (next.uuid().equals(key)) {
                    return next;
                }
            }
            return null;
        }
    }

    protected class MatchingResultSet<O extends Entity, T extends Query<EntityHandle<O>>> extends
            ResultSet<EntityHandle<O>> {
        private final Iterator<EntityHandle<O>> iterator;
        @Getter
        private final T query;
        @Getter
        private final QueryOptions queryOptions;
        private final int finalSize;

        public MatchingResultSet(Iterator<EntityHandle<O>> iterator, T query,
                                 QueryOptions queryOptions, int finalSize) {
            this.iterator = iterator;
            this.query = query;
            this.queryOptions = queryOptions;
            this.finalSize = finalSize;
        }

        @Override
        public Iterator<EntityHandle<O>> iterator() {
            return iterator;
        }

        @Override
        @SneakyThrows
        public boolean contains(EntityHandle<O> object) {
            try (Connection c = getDataSource().getConnection()) {
                String sql = "SELECT count(key) FROM " + getTableName() + " WHERE object = ?::UUID";
                try (PreparedStatement s = c.prepareStatement(sql)) {
                    try (java.sql.ResultSet resultSet = s.executeQuery()) {
                        resultSet.next();
                        return resultSet.getInt(1) > 0;
                    }
                }
            }
        }

        @Override
        public boolean matches(EntityHandle<O> object) {
            return query.matches(object, queryOptions);
        }

        @Override
        public int getRetrievalCost() {
            return indexRetrievalCost();
        }

        @Override
        public int getMergeCost() {
            return finalSize;
        }

        @Override
        public int size() {
            return finalSize;
        }

        @Override
        public void close() {
            if (iterator instanceof PostgreSQLStatementIterator) {
                ((PostgreSQLStatementIterator) iterator).close();
            }
        }
    }

    private class HasResultSet<O extends Entity> extends ResultSet<EntityHandle<O>> {
        private final PostgreSQLStatementIterator<EntityHandle<O>> iterator;
        private final Has<EntityHandle<O>, A> has;
        private final QueryOptions queryOptions;
        private final int finalSize;

        public HasResultSet(PostgreSQLStatementIterator<EntityHandle<O>> iterator, Has<EntityHandle<O>, A> has,
                            QueryOptions queryOptions, int finalSize) {
            this.iterator = iterator;
            this.has = has;
            this.queryOptions = queryOptions;
            this.finalSize = finalSize;
        }

        @Override
        public Iterator<EntityHandle<O>> iterator() {
            return iterator;
        }

        @Override
        @SneakyThrows
        public boolean contains(EntityHandle<O> object) {
            try (Connection c = getDataSource().getConnection()) {
                String sql = "SELECT count(key) FROM " + getTableName();
                try (PreparedStatement s = c.prepareStatement(sql)) {
                    try (java.sql.ResultSet resultSet = s.executeQuery()) {
                        resultSet.next();
                        return resultSet.getInt(1) > 0;
                    }
                }
            }
        }

        @Override
        public boolean matches(EntityHandle<O> object) {
            return has.matches(object, queryOptions);
        }

        @Override
        public Query<EntityHandle<O>> getQuery() {
            return has;
        }

        @Override
        public QueryOptions getQueryOptions() {
            return queryOptions;
        }

        @Override
        public int getRetrievalCost() {
            return indexRetrievalCost();
        }

        @Override
        public int getMergeCost() {
            return finalSize;
        }

        @Override
        public int size() {
            return finalSize;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

    protected abstract int indexRetrievalCost();

    protected A getQuantizedValue(A attributeValue) {
        return attributeValue;
    }

    protected enum OnConflictDo {
        UPDATE, NOTHING
    }

}
