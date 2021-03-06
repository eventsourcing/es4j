/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.queries;

import com.eventsourcing.*;
import com.eventsourcing.hlc.HybridTimestamp;
import com.eventsourcing.index.Index;
import com.eventsourcing.index.SimpleIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.Accessors;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.eventsourcing.index.IndexEngine.IndexFeature.EQ;
import static com.eventsourcing.index.IndexEngine.IndexFeature.GT;
import static com.eventsourcing.index.IndexEngine.IndexFeature.LT;
import static com.eventsourcing.queries.QueryFactory.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MinMaxTest extends RepositoryUsingTest {


    @Value
    @EqualsAndHashCode(callSuper = false)
    @Accessors(fluent = true)
    public static class TestEvent extends StandardEvent {

        String prop;

        public final static SimpleIndex<TestEvent, String> PROP = SimpleIndex.as(TestEvent::prop);

        @Index({EQ, LT, GT})
        public final static SimpleIndex<TestEvent, HybridTimestamp> TIMESTAMP = SimpleIndex.as(StandardEntity::timestamp);
    }

    @Value
    @EqualsAndHashCode(callSuper = false)
    @Accessors(fluent = true)
    public static class TestCommand extends StandardCommand<UUID, HybridTimestamp> {

        String prop;

        @Override public EventStream<UUID> events() throws Exception {
            TestEvent event = new TestEvent(prop);
            return EventStream.ofWithState(event.uuid(), event);
        }

        @Override public HybridTimestamp result(UUID state, Repository repository) {
            return repository.getJournal().get(state).get().timestamp();
        }
    }
    public MinMaxTest() {
        super(Min.class.getPackage());
    }

    @Test
    @SneakyThrows
    public void test() {
        HybridTimestamp ts1 = repository.publish(new TestCommand("test")).get();
        HybridTimestamp ts2 = repository.publish(new TestCommand("test")).get();
        HybridTimestamp ts3 = repository.publish(new TestCommand("test")).get();

        try (ResultSet<EntityHandle<TestEvent>> rs = repository.query(TestEvent.class, min(TestEvent.TIMESTAMP))) {
            assertEquals(rs.uniqueResult().get().timestamp(), ts1);
        }

        try (ResultSet<EntityHandle<TestEvent>> rs = repository.query(TestEvent.class, max(TestEvent.TIMESTAMP))) {
            assertEquals(rs.uniqueResult().get().timestamp(), ts3);
        }
    }

    @Test
    @SneakyThrows
    public void empty() {

        try (ResultSet<EntityHandle<TestEvent>> rs = repository.query(TestEvent.class, min(TestEvent.TIMESTAMP))) {
            assertTrue(rs.isEmpty());
        }

    }

    @Test @SneakyThrows
    public void testMassive() {
        UUID uuid = UUID.randomUUID();
        for (int i = 0; i < 100000; i++ ) {
            repository.publish(new IsLatestEntityTest.TestCommand("test" + (i + 1), uuid)).get();
        }
        Query<EntityHandle<IsLatestEntityTest.TestEvent>> query = scoped(equal(IsLatestEntityTest.TestEvent
                                                                                       .REFERENCE_ID, uuid),
                                                                         max(IsLatestEntityTest.TestEvent.TIMESTAMP));
        long t1 = System.nanoTime();
        try (ResultSet<EntityHandle<IsLatestEntityTest.TestEvent>> resultSet = repository.query(IsLatestEntityTest.TestEvent.class, query)) {
            assertEquals(resultSet.size(), 1);
            assertEquals(resultSet.uniqueResult().get().test(), "test100000");
            long t2 = System.nanoTime();
            long time = TimeUnit.SECONDS.convert(t2 - t1, TimeUnit.NANOSECONDS);
            if (time > 1) {
                System.err.println("Warning: [MinMaxTest.testMassive] is slow, took " + time +
                                           " seconds");
            }
        }
    }
}