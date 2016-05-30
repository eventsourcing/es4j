/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing;

import com.google.common.util.concurrent.Service;
import com.googlecode.cqengine.index.support.CloseableIterator;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
     * Journal <code>command</code> in repository <code>repository</code>,
     * with a default (no-op) listener ({@link #DEFAULT_LISTENER})
     * <p>
     * See more details at {@link #journal(Command, Listener)}
     *
     * @param command
     * @return number of events processed
     */
    default long journal(Command<?> command) throws Exception {
        return journal(command, DEFAULT_LISTENER);
    }

    /**
     * Journal <code>command</code> in repository <code>repository</code>,
     * with a custom listener.
     * <p>
     * Journal implementation should not make events visible to any other
     * readers until all events and the command have been persisted.
     *
     * @param command
     * @param listener
     * @return number of events processed
     */
    default long journal(Command<?> command, Listener listener) throws Exception {
        return journal(command, listener, new MemoryLockProvider());
    }

    /**
     * Journal <code>command</code> in repository <code>repository</code>,
     * with a custom listener and a lock provider.
     *
     * @param command
     * @param listener
     * @param lockProvider
     * @return number of events processed
     */
    long journal(Command<?> command, Listener listener, LockProvider lockProvider) throws Exception;

    default void flush() {}

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
    <T extends Command<?>> CloseableIterator<EntityHandle<T>> commandIterator(Class<T> klass);

    /**
     * Iterate over events of a specific type (through {@code EntityHandler<T>})
     *
     * @param klass
     * @param <T>
     * @return iterator
     */
    <T extends Event> CloseableIterator<EntityHandle<T>> eventIterator(Class<T> klass);

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
     * Journalling listener. Useful for observing progress.
     */
    interface Listener {
        /**
         * Called when a new event is received from the stream and
         * persisted into the journal. Note that at this point the event
         * should not be available until the entire journalling operation has
         * been completed and {@link #onCommit()} has been called.
         *
         * @param event
         */
        default void onEvent(Event event) {}

        /**
         * Called when all events and the command were persisted into the journal.
         * At this point, all records have been committed and are visible to all
         * other readers.
         */
        default void onCommit() {}

        /**
         * Called when there was an exception during event generation
         *
         * @param throwable
         */
        default void onAbort(Throwable throwable) {}
    }
}
