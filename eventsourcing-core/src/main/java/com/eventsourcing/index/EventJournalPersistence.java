/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.index;

import com.eventsourcing.EntityHandle;
import com.eventsourcing.Event;
import com.eventsourcing.Journal;
import com.eventsourcing.queries.options.EagerFetching;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.option.QueryOptions;

public class EventJournalPersistence<T extends Event> extends JournalPersistence<T> {

    public EventJournalPersistence(Journal journal, Class<T> klass) {
        super(journal, klass);
    }

    @Override
    public ObjectStore<EntityHandle<T>> createObjectStore() {
        return new EventJournalObjectStore<>(journal, klass);
    }

    @Override
    public boolean supportsIndex(Index<EntityHandle<T>> index) {
        return true;
    }

    @Override
    public void openRequestScopeResources(QueryOptions queryOptions) {
        queryOptions.put(EagerFetching.class, true);
    }

    @Override
    public void closeRequestScopeResources(QueryOptions queryOptions) {
        queryOptions.remove(EagerFetching.class);
    }

    static class EventJournalObjectStore<T extends Event> extends JournalObjectStore<T> {

        public EventJournalObjectStore(Journal journal, Class<T> klass) {
            super(journal, klass);
        }

        @Override
        public CloseableIterator<EntityHandle<T>> iterator(QueryOptions queryOptions) {
            return journal.eventIterator(klass, queryOptions);
        }
    }
}
