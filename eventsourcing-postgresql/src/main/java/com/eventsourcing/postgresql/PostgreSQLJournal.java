/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.postgresql;

import com.eventsourcing.*;
import com.eventsourcing.hlc.HybridTimestamp;
import com.eventsourcing.queries.options.EagerFetching;
import com.eventsourcing.layout.*;
import com.eventsourcing.layout.binary.BinarySerialization;
import com.eventsourcing.queries.options.NotSeenBy;
import com.google.common.base.Joiner;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.AbstractService;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.impossibl.postgres.jdbc.PGDataSource;
import com.zaxxer.hikari.HikariConfig;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import javax.sql.DataSource;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.eventsourcing.postgresql.PostgreSQLSerialization.*;

@Component(property = "type=PostgreSQLJournal", service = Journal.class)
public class PostgreSQLJournal extends AbstractService implements Journal {

    public static final int MAX_FETCH_SIZE = 10_000;
    @Reference
    protected DataSourceProvider dataSourceProvider;

    private HikariConfig hikariConfig;
    private DataSource dataSource;

    @Getter
    private Repository repository;
    private EntityLayoutExtractor entityLayoutExtractor = new EntityLayoutExtractor();

    @Override public void setRepository(Repository repository) {
        this.repository = repository;
        PooledDataSource pooledDataSource = PooledDataSource.getInstance(repository);
        if (hikariConfig != null) {
            pooledDataSource.setHikariConfig(hikariConfig);
        }
        pooledDataSource.getHikariConfig().setDataSource(dataSourceProvider.getDataSource());
        dataSource = pooledDataSource.getDataSource();
    }

    public PostgreSQLJournal() {}
    public PostgreSQLJournal(PGDataSource dataSource) {
        this.dataSourceProvider = () -> dataSource;
    }
    public PostgreSQLJournal(PGDataSource dataSource, HikariConfig hikariConfig) {
        this.dataSourceProvider = () -> dataSource;
        this.hikariConfig = hikariConfig;
    }

    @Override public void onCommandsAdded(Set<Class<? extends Command>> commands) {
        commands.forEach(entityLayoutExtractor);
    }

    @Override public void onEventsAdded(Set<Class<? extends Event>> events) {
        events.forEach(entityLayoutExtractor);
    }

    @Value
    static class Transaction implements Journal.Transaction {
        private final Connection connection;
        private final Savepoint savepoint;

        @SneakyThrows
        public Transaction(DataSource dataSource) {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            savepoint = connection.setSavepoint();
        }


        @SneakyThrows
        @Override public void commit() {
            connection.releaseSavepoint(savepoint);
            connection.commit();
            connection.close();
        }

        @SneakyThrows
        @Override public void rollback() {
            connection.rollback(savepoint);
            connection.releaseSavepoint(savepoint);
            connection.close();
        }
    }

    @Override public Journal.Transaction beginTransaction() {
        return new Transaction(dataSource);
    }

    private class PostgreSQLJournalProperties implements Properties {

        private final BinarySerialization serialization = BinarySerialization.getInstance();
        private final ObjectDeserializer<HybridTimestamp> timestampDeserializer = serialization.getDeserializer(HybridTimestamp.class);
        private final ObjectSerializer<HybridTimestamp> timestampSerializer = serialization.getSerializer(HybridTimestamp.class);

        private boolean initialized = false;

        @SneakyThrows
        private void ensureInitialized() {
            if (!initialized && dataSource != null) {
                try (Connection c = dataSource.getConnection()) {
                    String sql = "CREATE TABLE IF NOT EXISTS properties_v1 (name VARCHAR(255) UNIQUE, " + "val BYTEA)";
                    try (PreparedStatement s = c.prepareStatement(sql)) {
                        s.executeUpdate();
                    }
                }
                initialized = true;
            }
        }

        @SneakyThrows
        @Override public Optional<HybridTimestamp> getRepositoryTimestamp() {
            ensureInitialized();
            try (Connection c = dataSource.getConnection()) {
                String sql = "SELECT val FROM properties_v1 WHERE name = ?";
                try (PreparedStatement s = c.prepareStatement(sql)) {
                    s.setString(1, "repository_timestamp");
                    try (ResultSet rs = s.executeQuery()) {
                        if (rs.next()) {
                            HybridTimestamp ts = timestampDeserializer
                                    .deserialize(ByteBuffer.wrap(rs.getBytes(1)));
                            return Optional.of(ts);
                        } else {
                            return Optional.empty();
                        }
                    }
                }
            }
        }

        @SneakyThrows
        @Override public void setRepositoryTimestamp(HybridTimestamp timestamp) {
            ensureInitialized();
            try (Connection c = dataSource.getConnection()) {
                String sql = "INSERT INTO properties_v1 (name, val) VALUES (?, ?) ON CONFLICT (name) DO" +
                             " UPDATE SET val = ? WHERE properties_v1.name = ?";
                try (PreparedStatement s = c.prepareStatement(sql)) {
                    ByteBuffer serialized = timestampSerializer.serialize(timestamp);
                    s.setString(1, "repository_timestamp"); // insert
                    s.setString(4, "repository_timestamp"); // update
                    s.setBytes(2, serialized.array()); // insert
                    s.setBytes(3, serialized.array()); // update
                    s.executeUpdate();
                }
            }
        }
    }
    private Properties properties;

    @Override public Properties getProperties() {
        if (properties == null) {
            properties = new PostgreSQLJournalProperties();
        }
        return properties;
    }

    @Override public <S, T> Command<S, T> journal(Journal.Transaction tx, Command<S, T> command) {
        Layout layout = getLayout(command.getClass());
        String encoded = BaseEncoding.base16().encode(layout.getHash());
        insertFunctions.get(encoded).apply(command, ((Transaction)tx).getConnection());
        BinarySerialization serialization = BinarySerialization.getInstance();
        ByteBuffer s = serialization.getSerializer(command.getClass()).serialize(command);
        s.rewind();
        Command command1 = (Command) serialization.getDeserializer(command.getClass()).deserialize(s);
        command1.uuid(command.uuid());
        return command1;
    }

    @Override public Event journal(Journal.Transaction tx, Event event) {
        Layout layout = getLayout(event.getClass());
        String encoded = BaseEncoding.base16().encode(layout.getHash());
        InsertFunction insert = insertFunctions.get(encoded);
        insert.apply(event, ((Transaction)tx).getConnection());
        BinarySerialization serialization = BinarySerialization.getInstance();
        ByteBuffer s = serialization.getSerializer(event.getClass()).serialize(event);
        s.rewind();
        Event event1 = (Event) serialization.getDeserializer(event.getClass()).deserialize(s);
        event1.uuid(event.uuid());
        return event1;
    }

    @SneakyThrows
    @Override public <T extends Entity> Optional<T> get(UUID uuid) {
            Optional<T> result;
        Connection connection = dataSource.getConnection();
        refreshConnectionRegistry(connection);
        PreparedStatement s = connection
                .prepareStatement("SELECT layout FROM layouts_v1 WHERE uuid = ?::UUID");
        s.setString(1, uuid.toString());
        try (ResultSet resultSet = s.executeQuery()) {
            if (resultSet.next()) {
                byte[] bytes = resultSet.getBytes(1);
                String hash = BaseEncoding.base16().encode(bytes);
                ReaderFunction reader = readerFunctions.get(hash);
                Layout<?> layout = getLayout(bytes);
                String columns = Joiner.on(", ")
                                       .join(layout.getProperties().stream()
                                                   .map(p -> "\"" + p.getName() + "\"").collect(Collectors.toList()));
                String query = "SELECT " + columns + " FROM layout_v1_" + hash + " WHERE uuid = ?::UUID";

                PreparedStatement s1 = connection.prepareStatement(query);
                s1.setString(1, uuid.toString());

                try (ResultSet rs = s1.executeQuery()) {
                    rs.next();
                    Entity o = (Entity) reader.apply(rs);
                    o.uuid(uuid);
                    result = Optional.of((T) o);
                }
                s1.close();
            } else {
                result = Optional.empty();
            }
        }
        s.close();
        connection.close();
        return result;
    }

    @Override public <T extends Command<?, ?>> CloseableIterator<EntityHandle<T>> commandIterator(Class<T> klass,
                                                                                                  QueryOptions queryOptions) {
        return entityIterator(klass, queryOptions);
    }

    @Override public <T extends Event> CloseableIterator<EntityHandle<T>> eventIterator(Class<T> klass, QueryOptions
            queryOptions) {
        return entityIterator(klass, queryOptions);
    }

    @SneakyThrows
    private <T extends Entity> CloseableIterator<EntityHandle<T>> entityIterator(Class<T> klass, QueryOptions queryOptions) {
        boolean eagerFetching = queryOptions.get(EagerFetching.class) != null;
        Connection connection = dataSource.getConnection();

        Layout<?> layout = getLayout(klass);
        String hash = BaseEncoding.base16().encode(layout.getHash());

        String join = " LEFT JOIN seenby_v1 ON seenby_v1.layout = ? AND seenby_v1.seen_by = ? WHERE " +
                          "___id___ > COALESCE(seenby_v1.seen, 0)";
        if (!eagerFetching) {
            PreparedStatement s = connection.prepareStatement("SELECT uuid, ___id___  FROM layout_v1_" + hash + join);
            s.setFetchSize(MAX_FETCH_SIZE);
            s.setBytes(1, layout.getHash());
            NotSeenBy notSeenBy = queryOptions.get(NotSeenBy.class);
            s.setBytes(2, notSeenBy == null ? new byte[]{} : notSeenBy.getId());
            return new EntityIterator<>(this, s, connection, queryOptions, layout);
        } else {
            ReaderFunction reader = readerFunctions.get(hash);
            List<? extends Property<?>> properties = layout.getProperties();
            String columns = Joiner.on(", ").join(properties.stream()
                                                        .map(p -> "t.\"" + p.getName() + "\"").collect(Collectors
                                                                                                              .toList()));
            String query = "SELECT " + columns + ", uuid AS ___uuid___, ___id___ FROM layout_v1_" + hash + " AS t" + join;
            PreparedStatement s = connection.prepareStatement(query);
            s.setFetchSize(MAX_FETCH_SIZE);
            s.setBytes(1, layout.getHash());
            NotSeenBy notSeenBy = queryOptions.get(NotSeenBy.class);
            s.setBytes(2, notSeenBy == null ? new byte[]{} : notSeenBy.getId());
            return new EagerEntityIterator<>(this, s, connection, reader, queryOptions, layout);
        }
    }

    static private class SeenByListener<R extends Entity> extends PostgreSQLStatementIterator
            .Listener<EntityHandle<R>> {

        private final byte[] identifier;
        private final Connection connection;
        private final Layout<?> layout;
        private BigInteger lastSeen;

        public SeenByListener(Connection connection, QueryOptions queryOptions, Layout<?> layout) {
            this.connection = connection;
            this.layout = layout;
            NotSeenBy notSeenBy = queryOptions.get(NotSeenBy.class);
            if (notSeenBy != null) {
                identifier = notSeenBy.getId();
            } else {
                identifier = null;
            }
        }

        @SneakyThrows
        @Override public void resultSetConsumed(ResultSet resultSet, EntityHandle<R> rEntityHandle) {
            if (identifier != null) {
                BigInteger next = resultSet.getBigDecimal("___id___").toBigInteger();
                if (lastSeen == null || next.compareTo(lastSeen) > 0) {
                    lastSeen = next;
                }
            }
        }

        @SneakyThrows
        @Override public void resultSetClosed() {
            if (identifier != null && lastSeen != null) {
                try (PreparedStatement s = connection
                        .prepareStatement("INSERT INTO seenby_v1 (layout, seen_by, seen) VALUES (?, ?, ?) " +
                                          " ON CONFLICT (layout, seen_by) DO UPDATE SET seen = ?")) {
                    s.setBytes(1, layout.getHash());
                    s.setBytes(2, identifier);
                    s.setBigDecimal(3, new BigDecimal(lastSeen));
                    s.setBigDecimal(4, new BigDecimal(lastSeen));
                    s.executeUpdate();
                }
            }
        }
    }

    static private class EntityIterator<R extends Entity> extends PostgreSQLStatementIterator<EntityHandle<R>> {

        private final Journal journal;

        public EntityIterator(Journal journal, PreparedStatement statement,
                              Connection connection, QueryOptions queryOptions, Layout<?> layout) {
            super(statement, connection, true);
            setListener(new SeenByListener<>(connection, queryOptions, layout));
            this.journal = journal;
        }

        @SneakyThrows
        @Override
        public EntityHandle<R> fetchNext() {
            return new JournalEntityHandle<>(journal, UUID.fromString(resultSet.getString(1)));
        }
    }

    static private class EagerEntityIterator<R extends Entity> extends PostgreSQLStatementIterator<EntityHandle<R>> {

        private final Journal journal;
        private final ReaderFunction reader;

        public EagerEntityIterator(Journal journal, PreparedStatement statement,
                                   Connection connection, ReaderFunction reader, QueryOptions queryOptions,
                                   Layout<?> layout) {
            super(statement, connection, true);
            setListener(new SeenByListener<>(connection, queryOptions, layout));
            this.journal = journal;
            this.reader = reader;
        }

        @SneakyThrows
        @Override
        public EntityHandle<R> fetchNext() {
            Entity<?> o = (Entity) reader.apply(resultSet);
            o.uuid(UUID.fromString(resultSet.getString("___uuid___")));
            return new ResolvedEntityHandle<>((R) o);
        }
    }


    @SneakyThrows
    @Override public void clear() {
        Connection connection = dataSource.getConnection();
        layoutsByHash.keySet().forEach(new Consumer<String>() {
            @SneakyThrows
            @Override public void accept(String hash) {
                PreparedStatement s = connection.prepareStatement("DELETE FROM layout_v1_" + hash);
                s.execute();
                s.close();
            }
        });
        PreparedStatement check = connection
                .prepareStatement("SELECT * from pg_catalog.pg_tables WHERE tablename = 'layouts' AND schemaname = ?");
        check.setString(1, "eventsourcing");
        try (ResultSet resultSet = check.executeQuery()) {
            if (resultSet.next()) {
                PreparedStatement s = connection.prepareStatement("DELETE FROM layouts_v1");
                s.execute();
                s.close();
            }
        }
        check.close();
        connection.close();
    }

    @SneakyThrows
    @Override public <T extends Entity> long size(Class<T> klass) {
        Layout layout = getLayout(klass);
        String hash = BaseEncoding.base16().encode(layout.getHash());
        Connection connection = dataSource.getConnection();
        PreparedStatement s = connection
                .prepareStatement("SELECT count(uuid) FROM layout_v1_" + hash);

        long size;
        try (ResultSet resultSet = s.executeQuery()) {
            resultSet.next();
            size = resultSet.getLong(1);
        }

        s.close();
        connection.close();
        return size;
    }

    @Override public <T extends Entity> boolean isEmpty(Class<T> klass) {
        return size(klass) == 0;
    }

    @Override protected void doStart() {
        if (repository == null) {
            notifyFailed(new IllegalStateException("repository == null"));
        }

        if (dataSource == null) {
            notifyFailed(new IllegalStateException("dataSource == null"));
        }

        ensureLatestSchemaVersion();

        notifyStarted();
    }

    @SneakyThrows
    private void ensureLatestSchemaVersion() {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement s = connection
                    .prepareStatement("CREATE TABLE IF NOT EXISTS layouts_v1 (\n" +
                                              "  uuid   UUID PRIMARY KEY,\n" +
                                              "  layout BYTEA NOT NULL\n" +
                                              ")")) {
                s.executeUpdate();
            }
            String timestampFunction = CharStreams.toString(new InputStreamReader(getClass().getResourceAsStream
                    ("timestamp_function.sql")));
            try (PreparedStatement s = connection.prepareStatement(timestampFunction)) {
                s.executeUpdate();
            }
            try (PreparedStatement s = connection
                    .prepareStatement("CREATE TABLE IF NOT EXISTS seenby_v1 (\n" +
                                              "  layout  BYTEA NOT NULL,\n" +
                                              "  seen_by BYTEA NOT NULL,\n" +
                                              "  seen    SERIAL8 NOT NULL,\n" +
                                              "  PRIMARY KEY (\"layout\", \"seen_by\")\n" +
                                              ")")) {
                s.executeUpdate();
            }

        }
    }

    @Override protected void doStop() {
        notifyStopped();
    }

    private Map<String, InsertFunction> insertFunctions = new ConcurrentHashMap<>();
    private Map<String, ReaderFunction> readerFunctions = new ConcurrentHashMap<>();

    private class ReaderFunction implements Function<ResultSet, Object> {

        private final Layout layout;

        public ReaderFunction(Layout<?> layout) {
            this.layout = layout;
        }

        @SneakyThrows
        @Override public Object apply(ResultSet resultSet) {
            AtomicInteger i = new AtomicInteger(1);
            List<? extends Property<?>> properties = layout.getProperties();
            Map<Property<?>, Object> props = new HashMap<>();
            for (Property property : properties) {
                TypeHandler typeHandler = property.getTypeHandler();
                props.put(property, getValue(resultSet, i, typeHandler));
            }

            return layout.instantiate(props);
        }

    }

    private class InsertFunction implements BiFunction<Object, Connection, UUID> {
        private final Layout<?> layout;
        private final String table;
        private final List<? extends Property> properties;

        public InsertFunction(Layout<?> layout) {
            this.layout = layout;
            table = "layout_v1_" +  BaseEncoding.base16().encode(layout.getHash());
            properties = layout.getProperties();
        }

        @SneakyThrows
        @Override public UUID apply(Object object, Connection connection) {
            String parameters = Joiner.on(",")
                               .join(properties.stream()
                                               .map(p -> getParameter(connection, p.getTypeHandler(), p.get(object)))
                                               .collect(Collectors.toList()));

            PreparedStatement s = connection
                    .prepareStatement("INSERT INTO " + table + " VALUES (?::UUID," + parameters + ")");
            int i = 1;
            UUID uuid;
            if (object instanceof Entity) {
                uuid = ((Entity) object).uuid();
            } else {
                uuid = UUID.randomUUID();
            }
            s.setString(i, uuid.toString());
            i++;
            for (Property property : layout.getProperties()) {
                Object value = property.get(object);
                i = setValue(connection, s, i, value, property.getTypeHandler());
            }
            s.execute();

            PreparedStatement layoutsInsertion = connection.prepareStatement("INSERT INTO layouts_v1 " +
                                                                                     "VALUES (?::UUID, " +
                                                                                     "?)");
            layoutsInsertion.setString(1, uuid.toString());
            layoutsInsertion.setBytes(2, layout.getHash());
            layoutsInsertion.executeUpdate();

            s.close();
            return uuid;
        }


    }

    private Map<String, Layout> layoutsByClass = new ConcurrentHashMap<>();
    private Map<String, Layout> layoutsByHash = new ConcurrentHashMap<>();

    private Layout getLayout(Class<? extends Entity> klass) {
        if (!layoutsByClass.containsKey(klass.getName())) {
            entityLayoutExtractor.accept(klass);
        }
        return layoutsByClass.get(klass.getName());
    }

    private Layout getLayout(byte[] hash) {
        String encoded = BaseEncoding.base16().encode(hash);
        return layoutsByHash.get(encoded);
    }


    private class EntityLayoutExtractor implements Consumer<Class<? extends Entity>> {
        @SneakyThrows
        @Override public void accept(Class<? extends Entity> aClass) {
            Layout<?> layout = Layout.forClass(aClass);
            layoutsByClass.put(aClass.getName(), layout);
            byte[] fingerprint = layout.getHash();
            String encoded = BaseEncoding.base16().encode(fingerprint);
            if (!layoutsByHash.containsKey(encoded)) {
                layoutsByHash.put(encoded, layout);
                try (Connection connection = dataSource.getConnection()) {

                    String columns = defineColumns(connection, layout);

                    String createTable = "CREATE TABLE IF NOT EXISTS layout_v1_" + encoded + " (" + "uuid UUID PRIMARY KEY," + columns + ")";

                    try (PreparedStatement s = connection.prepareStatement(createTable)) {
                        s.execute();
                    }

                    // We are not changing v1 here because it's just an extension of v1, the existing columns
                    // were not changed. This serial ID is necessary to enable NotSeenBy query option (and potentially
                    // others)
                    String addId = "ALTER TABLE layout_v1_" + encoded + " ADD COLUMN IF NOT EXISTS ___id___ BIGSERIAL UNIQUE";

                    try (PreparedStatement s = connection.prepareStatement(addId)) {
                        s.execute();
                    }

                    String comment = "COMMENT ON TABLE layout_v1_" + encoded + " IS '" + layout.getName() + "'";
                    try (PreparedStatement s = connection.prepareStatement(comment)) {
                        s.execute();
                    }

                }

                InsertFunction insertFunction = new InsertFunction(layout);
                insertFunctions.put(encoded, insertFunction);

                ReaderFunction readerFunction = new ReaderFunction(layout);
                readerFunctions.put(encoded, readerFunction);
            }
        }

    }

    protected static String defineColumns(Connection connection, Layout<?> layout) {
        return Joiner.on(",\n").join(layout.getProperties().stream()
                                           .map(p -> "\"" + p.getName() + "\" " +
                                                          PostgreSQLSerialization.getMappedType(connection, p.getTypeHandler()))
                                           .collect(Collectors.toList()));
    }


}
