/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.cep.protocols;

import com.eventsourcing.EntityHandle;
import com.eventsourcing.Protocol;
import com.eventsourcing.Repository;
import com.eventsourcing.cep.events.NameChanged;
import com.eventsourcing.queries.ModelCollectionQuery;
import com.eventsourcing.queries.ModelLoader;
import com.eventsourcing.queries.ModelQueries;
import com.googlecode.cqengine.resultset.ResultSet;
import org.unprotocols.coss.Draft;
import org.unprotocols.coss.RFC;

import java.util.Optional;
import java.util.stream.Stream;

import static com.eventsourcing.index.EntityQueryFactory.equal;
import static com.googlecode.cqengine.stream.StreamFactory.streamOf;

@Draft @RFC(url = "http://rfc.eventsourcing.com/spec:3/CEP")
public interface NameProtocol extends Protocol, ModelQueries {
    default String name() {
        Optional<NameChanged> last = latestAssociatedEntity(NameChanged.class, NameChanged.REFERENCE_ID, NameChanged.TIMESTAMP);
        if (last.isPresent()) {
            return last.get().name();
        } else {
            return null;
        }
    }

    class NamedModelCollectionQuery<T extends NameProtocol> implements ModelCollectionQuery<T> {

        private final String name;
        private final ModelLoader<T> loader;

        public NamedModelCollectionQuery(String name, ModelLoader<T> loader) {
            this.name = name;
            this.loader = loader;
        }

        @Override public Stream<T> getCollectionStream(Repository repository) {
            ResultSet<EntityHandle<NameChanged>> resultSet = repository
                    .query(NameChanged.class, equal(NameChanged.NAME, name));
            return streamOf(resultSet)
                    .map(h -> loader.load(repository, h.get().reference()).get())
                    .onClose(resultSet::close);
        }
    }

    static <T extends NameProtocol> ModelCollectionQuery<T> named(String name, ModelLoader<T> loader) {
        return new NamedModelCollectionQuery<>(name, loader);
    }
}
