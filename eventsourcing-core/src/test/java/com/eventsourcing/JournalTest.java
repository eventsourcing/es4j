/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing;

import com.eventsourcing.hlc.HybridTimestamp;
import com.eventsourcing.hlc.NTPServerTimeProvider;
import com.eventsourcing.index.IndexEngine;
import com.eventsourcing.index.MemoryIndexEngine;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.testng.Assert.*;

public abstract class JournalTest<T extends Journal> {

    protected T journal;
    private RepositoryImpl repository;
    private IndexEngine indexEngine;
    protected NTPServerTimeProvider timeProvider;

    @SneakyThrows
    public JournalTest(T journal) {
        this.journal = journal;
    }

    @BeforeClass
    public void setUpEnv() throws Exception {
        repository = new RepositoryImpl();
        repository.addCommandSetProvider(new PackageCommandSetProvider(new Package[]{JournalTest.class.getPackage()}));
        repository.addEventSetProvider(new PackageEventSetProvider(new Package[]{JournalTest.class.getPackage()}));
        repository.setJournal(this.journal);
        timeProvider = new NTPServerTimeProvider(new String[]{"localhost"});
        repository.setPhysicalTimeProvider(timeProvider);
        repository.setLockProvider(new MemoryLockProvider());
        indexEngine = new MemoryIndexEngine();
        repository.setIndexEngine(indexEngine);
        repository.startAsync().awaitRunning();
    }

    @AfterClass
    public void tearDownEnv() throws Exception {
        repository.stopAsync().awaitTerminated();
    }

    @BeforeMethod
    public void setUp() throws Exception {
        journal.clear();
    }

    public static class TestEvent extends Event {}

    public static class AnotherTestEvent extends Event {}

    @EqualsAndHashCode(callSuper = false)
    public static class TestCommand extends Command<Void> {
        @Getter @Setter
        private boolean events;

        public TestCommand() {
        }

        public TestCommand(boolean events) {
            this.events = events;
        }

        @Override
        public Stream<Event> events(Repository repository) throws Exception {
            if (events) {
                return Stream.of(new TestEvent());
            } else {
                return super.events(repository);
            }
        }
    }

    public static class ExceptionalTestCommand extends Command<Void> {
        @Override
        public Stream<Event> events(Repository repository) throws Exception {
            return Stream.generate((Supplier<Event>) () -> {
                throw new IllegalStateException();
            });
        }
    }

    @Test
    @SneakyThrows
    public void journalCounting() {
        HybridTimestamp timestamp = new HybridTimestamp(timeProvider);
        timestamp.update();
        assertEquals(journal.journal((Command<?>) new TestCommand(true).timestamp(timestamp)), 1);
        timestamp.update();
        assertEquals(journal.journal((Command<?>) new TestCommand(false).timestamp(timestamp)), 0);
    }

    @Test
    @SneakyThrows
    public void journalListener() {
        AtomicInteger onEvent = new AtomicInteger(0);
        AtomicBoolean onCommit = new AtomicBoolean(false);
        HybridTimestamp timestamp = new HybridTimestamp(timeProvider);
        timestamp.update();

        assertEquals(1,
                     journal.journal((Command<?>) new TestCommand(true).timestamp(timestamp), new Journal.Listener() {
                         @Override
                         public void onEvent(Event event) {
                             onEvent.incrementAndGet();
                         }

                         @Override
                         public void onCommit() {
                             onCommit.set(true);
                         }
                     }));

        assertEquals(onEvent.get(), 1);
        assertTrue(onCommit.get());
    }

    @Test
    @SneakyThrows
    public void journalListenerAbort() {
        AtomicBoolean onAbort = new AtomicBoolean(false);
        HybridTimestamp timestamp = new HybridTimestamp(timeProvider);
        timestamp.update();

        try {
            assertEquals(1, journal.journal((Command<?>) new ExceptionalTestCommand().timestamp(timestamp),
                                            new Journal.Listener() {
                                                @Override
                                                public void onAbort(Throwable throwable) {
                                                    onAbort.set(true);
                                                }
                                            }));
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }

        assertTrue(onAbort.get());
    }

    @Test
    @SneakyThrows
    public void journalRetrieving() {
        HybridTimestamp timestamp = new HybridTimestamp(timeProvider);
        timestamp.update();
        List<Event> events = new ArrayList<>();
        TestCommand command = new TestCommand(true);
        journal.journal((Command<?>) command.timestamp(timestamp), new Journal.Listener() {
            @Override
            public void onEvent(Event event) {
                events.add(event);
            }
        });
        assertEquals(events.size(), 1);

        journal.flush();

        Optional<Entity> entity = journal.get(command.uuid());
        assertTrue(entity.isPresent());
        assertEquals(command.uuid(), entity.get().uuid());

        Event event = events.get(0);
        Optional<Entity> eventEntity = journal.get(event.uuid());
        assertTrue(eventEntity.isPresent());
        assertEquals(event.uuid(), eventEntity.get().uuid());
    }

    @Test
    @SneakyThrows
    public void journalIterating() {
        HybridTimestamp timestamp = new HybridTimestamp(timeProvider);
        timestamp.update();
        List<Event> events = new ArrayList<>();
        TestCommand command = new TestCommand(true);
        journal.journal((Command<?>) command.timestamp(timestamp), new Journal.Listener() {
            @Override
            public void onEvent(Event event) {
                events.add(event);
            }
        });

        Iterator<EntityHandle<TestCommand>> commandIterator = journal.commandIterator(TestCommand.class);
        assertTrue(commandIterator.hasNext());
        assertEquals(commandIterator.next().uuid(), command.uuid());
        assertFalse(commandIterator.hasNext());

        assertEquals(events.size(), 1);
        Event event = events.get(0);

        Iterator<EntityHandle<TestEvent>> eventIterator = journal.eventIterator(TestEvent.class);
        assertTrue(eventIterator.hasNext());
        assertEquals(eventIterator.next().uuid(), event.uuid());
        assertFalse(eventIterator.hasNext());

        assertFalse(journal.eventIterator(AnotherTestEvent.class).hasNext());
    }

}