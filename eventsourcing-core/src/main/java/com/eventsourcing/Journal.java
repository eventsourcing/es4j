/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing;

import com.eventsourcing.hlc.HybridTimestamp;
import com.eventsourcing.queries.QueryFactory;
import com.eventsourcing.utils.CloseableWrappingIterator;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Service;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.query.option.QueryOptions;
import lombok.SneakyThrows;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.eventsourcing.queries.QueryFactory.noQueryOptions;

/**
 * Journal is the storage of all events and commands registered
 * through Eventsourcing.
 */
public interface Journal extends Service {

    Listener DEFAULT_LISTENER = new Listener() {};

    default void onCommandsAdded(Set<Class<? extends Command>> commands) {}

    default void onEventsAdded(Set<Class<? extends Event>> events) {}

    /**
     * Set repository. Should be done before invoking {@link #startAsync()}
     *
     * @param repository
     */
    void setRepository(Repository repository);

    /**
     * Get repository
     * @return
     */
    Repository getRepository();

    /**
     * Retrieves a command or event by UUID
     *
     * @param uuid
     * @param <T>
     * @return Empty {@link Optional} if neither a command nor an event are found by <code>uuid</code>
     */
    <T extends Entity> Optional<T> get(UUID uuid);

    /**
     * Iterate over commands of a specific type (through {@code EntityHandler<T>})
     *
     * @param klass
     * @param <T>
     * @return iterator
     */
    default <T extends Command<?, ?>> CloseableIterator<EntityHandle<T>> commandIterator(Class<T> klass) {
        return commandIterator(klass, noQueryOptions());
    }

    /**
     * Iterate over events of a specific type (through {@code EntityHandler<T>})
     *
     * @param klass
     * @param <T>
     * @return iterator
     */
    default <T extends Event> CloseableIterator<EntityHandle<T>> eventIterator(Class<T> klass) {
        return eventIterator(klass, noQueryOptions());
    }

    /**
     * Iterate over commands of a specific type (through {@code EntityHandler<T>})
     *
     * @param klass
     * @param <T>
     * @return iterator
     */
    <T extends Command<?, ?>> CloseableIterator<EntityHandle<T>> commandIterator(Class<T> klass,
                                                                                 QueryOptions queryOptions);

    /**
     * Iterate over events of a specific type (through {@code EntityHandler<T>})
     *
     * @param klass
     * @param <T>
     * @return iterator
     */
    <T extends Event> CloseableIterator<EntityHandle<T>> eventIterator(Class<T> klass, QueryOptions queryOptions);

    /**
     * Removes everything from the journal.
     * <p>
     * <b>Use with caution</b>: the data will be lost irrevocably
     */
    void clear();

    /**
     * Returns the count of entities of specified type
     *
     * @param klass
     * @param <T>
     * @return
     */
    <T extends Entity> long size(Class<T> klass);

    /**
     * Returns true if there are no entities of specified type stored
     *
     * @param klass
     * @param <T>
     * @return
     */
    <T extends Entity> boolean isEmpty(Class<T> klass);

    /**
     * Record command within a transaction
     * @param tx
     * @param command
     * @param <S>
     * @param <T>
     * @return
     */
    <S, T> Command<S, T> journal(Transaction tx, Command<S, T> command);

    /**
     * Record event within a transaction
     * @param tx
     * @param event
     * @return
     */
    Event journal(Transaction tx, Event event);

    /**
     * Starts a transaction
     * @return
     */
    Transaction beginTransaction();

    /**
     * An interface abstracting journal's transaction
     */
    interface Transaction {
        void commit();
        void rollback();
    }

    /**
     * Journal-stored "system" properties
     */
    interface Properties {
        Optional<HybridTimestamp> getRepositoryTimestamp();
        void setRepositoryTimestamp(HybridTimestamp timestamp);
    }

    /**
     * @return journal {@link Properties} object
     */
    Properties getProperties();

}
