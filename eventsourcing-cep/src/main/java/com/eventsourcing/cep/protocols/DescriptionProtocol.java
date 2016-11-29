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
import com.eventsourcing.cep.events.DescriptionChanged;
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
public interface DescriptionProtocol extends Protocol, ModelQueries {
    default String description() {
        Optional<DescriptionChanged> last = latestAssociatedEntity(DescriptionChanged.class, DescriptionChanged.REFERENCE_ID, DescriptionChanged.TIMESTAMP);
        if (last.isPresent()) {
            return last.get().description();
        } else {
            return null;
        }
    }

    class DescribedModelCollectionQuery<T extends DescriptionProtocol> implements ModelCollectionQuery<T> {

        private final String description;
        private final ModelLoader<T> loader;

        public DescribedModelCollectionQuery(String description, ModelLoader<T> loader) {
            this.description = description;
            this.loader = loader;
        }

        @Override public Stream<T> getCollectionStream(Repository repository) {
            ResultSet<EntityHandle<DescriptionChanged>> resultSet = repository
                    .query(DescriptionChanged.class, equal(DescriptionChanged.DESCRIPTION, description));
            return streamOf(resultSet)
                    .map(h -> loader.load(repository, h.get().reference()).get())
                    .onClose(resultSet::close);
        }
    }

    static <T extends DescriptionProtocol> ModelCollectionQuery<T> described(String description, ModelLoader<T> loader) {
        return new DescribedModelCollectionQuery<>(description, loader);
    }
}
