/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.index;

import com.eventsourcing.Entity;
import com.eventsourcing.EntityHandle;
import com.eventsourcing.Journal;
import com.eventsourcing.JournalEntityHandle;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.persistence.Persistence;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.util.Collection;
import java.util.UUID;

public abstract class JournalPersistence<T extends Entity> implements Persistence<EntityHandle<T>, UUID> {

    protected final Journal journal;
    protected final Class<T> klass;

    public JournalPersistence(Journal journal, Class<T> klass) {
        this.journal = journal;
        this.klass = klass;
    }

    @Override
    public SimpleAttribute<EntityHandle<T>, UUID> getPrimaryKeyAttribute() {
        return new SimpleAttribute<EntityHandle<T>, UUID>() {
            @Override
            public UUID getValue(EntityHandle<T> object, QueryOptions queryOptions) {
                return object.uuid();
            }
        };
    }


    static abstract class JournalObjectStore<T extends Entity> implements ObjectStore<EntityHandle<T>>,
            KeyObjectStore<UUID, EntityHandle<T>>
    {

        protected final Journal journal;
        protected final Class<T> klass;

        public JournalObjectStore(Journal journal, Class<T> klass) {
            this.journal = journal;
            this.klass = klass;
        }

        @Override public EntityHandle<T> get(UUID key) {
            return new JournalEntityHandle<>(journal, key);
        }

        @Override
        public int size(QueryOptions queryOptions) {
            return (int) journal.size(klass);
        }

        @Override
        public boolean isEmpty(QueryOptions queryOptions) {
            return journal.isEmpty(klass);
        }

        @Override
        public boolean contains(Object o, QueryOptions queryOptions) {
            return journal.get(((EntityHandle<T>) o).uuid()).isPresent();
        }

        @Override
        public boolean add(EntityHandle<T> tEntityHandle, QueryOptions queryOptions) {
            return true; // this is taken care of with journalling
        }

        @Override
        public boolean remove(Object o, QueryOptions queryOptions) {
            return false; // immutable set
        }

        @Override
        public boolean containsAll(Collection<?> c, QueryOptions queryOptions) {
            return c.stream().allMatch(v -> contains(v, queryOptions));
        }

        @Override
        public boolean addAll(Collection<? extends EntityHandle<T>> c, QueryOptions queryOptions) {
            return true; // this is taken care of with journalling
        }

        @Override
        public boolean retainAll(Collection<?> c, QueryOptions queryOptions) {
            return false; // immutable set
        }

        @Override
        public boolean removeAll(Collection<?> c, QueryOptions queryOptions) {
            return false; // immutable set
        }

        @Override
        public void clear(QueryOptions queryOptions) {
            // immutable set
        }
    }

}
