/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing;

import com.eventsourcing.hlc.HybridTimestamp;
import com.eventsourcing.hlc.PhysicalTimeProvider;
import com.eventsourcing.index.IndexEngine;
import com.eventsourcing.layout.Deserializer;
import com.eventsourcing.layout.Layout;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.googlecode.cqengine.IndexedCollection;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Slf4j
public class DisruptorCommandConsumer extends AbstractService implements CommandConsumer {


    private final Iterable<Class<? extends Command>> commandClasses;
    private final PhysicalTimeProvider timeProvider;
    private final Repository repository;
    private final Journal journal;
    private final IndexEngine indexEngine;
    private final LockProvider lockProvider;
    private final Map<Class<? extends Command>, Layout> layouts = new HashMap<>();
    private final Map<Class<? extends Command>, Deserializer> deserializers = new HashMap<>();

    private static class CommandEvent {
        boolean endOfBatch;
        @Getter @Setter
        Collection<EntitySubscriber> entitySubscribers = new ArrayList<>();
        Map<Class<? extends Command>, Command> commands = new HashMap<>();
        TrackingLockProvider lockProvider;
        CompletableFuture completed;
        private Class<? extends Command> commandClass;

        @SneakyThrows
        public CommandEvent(Iterable<Class<? extends Command>> classes) {
            for (Class<? extends Command> cmd : classes) {
                commands.put(cmd, cmd.newInstance());
            }
        }

        public void setCommandClass(Class<Command> klass) {
            this.commandClass = klass;
        }

        public Command getCommand() {
            return commands.computeIfAbsent(commandClass, new ClassCommandFunction());
        }


        private static class ClassCommandFunction implements Function<Class<? extends Command>, Command> {
            @Override @SneakyThrows
            public Command apply(Class<? extends Command> aClass) {
                return aClass.newInstance();
            }
        }
    }

    public static final int RING_BUFFER_SIZE = 1024;
    private RingBuffer<CommandEvent> ringBuffer;
    private Disruptor<CommandEvent> disruptor;

    private HybridTimestamp timestamp;

    private static class JournalListener implements Journal.Listener {
        private final CommandEvent disruptorEvent;
        private final IndexEngine indexEngine;
        private final Journal journal;
        private final Command<?> command;
        
        private final Map<EntitySubscriber, Set<UUID>> subscriptions = new HashMap<>();
        
        private JournalListener(CommandEvent event, IndexEngine indexEngine, Journal journal, Command<?> command) {
            this.disruptorEvent = event;
            this.indexEngine = indexEngine;
            this.journal = journal;
            this.command = command;
            disruptorEvent.getEntitySubscribers().forEach(s -> subscriptions.put(s, new HashSet<>()));
        }

        @Override @SuppressWarnings("unchecked")
        public void onEvent(Event event) {
            IndexedCollection<EntityHandle<Event>> coll = indexEngine
                    .getIndexedCollection((Class<Event>) event.getClass());
            coll.add(new JournalEntityHandle<>(journal, event.uuid()));
            disruptorEvent.getEntitySubscribers().stream()
                    .filter(s -> s.matches(event))
                    .forEach(s -> subscriptions.get(s).add(event.uuid()));
        }

        @Override @SuppressWarnings("unchecked")
        public void onCommit() {
            IndexedCollection<EntityHandle<Command<?>>> coll = indexEngine
                    .getIndexedCollection((Class<Command<?>>) command.getClass());
            EntityHandle<Command<?>> commandHandle = new JournalEntityHandle<>(journal, command.uuid());
            coll.add(commandHandle);
            subscriptions.entrySet().stream()
                    .forEach(entry -> entry.getKey()
                                           .accept(entry.getValue()
                                                        .stream()
                                                        .map(uuid -> new JournalEntityHandle<>(journal, uuid))));
            disruptorEvent.getEntitySubscribers().stream()
                    .filter(s -> s.matches(command))
                    .forEach(s -> s.accept(Stream.of(commandHandle)));
        }

        @Override
        public void onAbort(Throwable throwable) {

        }
    }

    private static class TrackingLockProvider extends AbstractService implements LockProvider {

        private final Set<Lock> locks = new HashSet<>();
        private final LockProvider lockProvider;

        private TrackingLockProvider(LockProvider lockProvider) {
            this.lockProvider = lockProvider;
        }

        private void release() {
            for (Lock lock : locks) {
                lock.unlock();
            }
        }

        @Override
        public Lock lock(Object lock) {
            Lock l = lockProvider.lock(lock);
            locks.add(l);
            return new TrackingLock(l);
        }

        @Override
        protected void doStart() {
            notifyStarted();
        }

        @Override
        protected void doStop() {
            notifyStopped();
        }

        class TrackingLock implements Lock {

            private final Lock lock;

            public TrackingLock(Lock lock) {
                this.lock = lock;
            }

            @Override
            public void unlock() {
                lock.unlock();
                locks.remove(lock);
            }

            @Override
            public boolean isLocked() {
                return lock.isLocked();
            }
        }
    }

    @SneakyThrows
    public DisruptorCommandConsumer(Iterable<Class<? extends Command>> commandClasses,
                                    PhysicalTimeProvider timeProvider,
                                    Repository repository, Journal journal, IndexEngine indexEngine,
                                    LockProvider lockProvider) {
        this.commandClasses = commandClasses;
        this.timeProvider = timeProvider;
        this.repository = repository;
        this.journal = journal;
        this.indexEngine = indexEngine;
        this.lockProvider = lockProvider;
        this.timestamp = new HybridTimestamp(timeProvider);
        for (Class<? extends Command> cmd : commandClasses) {
            Layout<? extends Command> layout = new Layout<>(cmd);
            layouts.put(cmd, layout);
            deserializers.put(cmd, new Deserializer<>(layout));
        }
    }

    private void timestamp(CommandEvent event, long sequence, boolean endOfBatch) throws Exception {
        timestamp.update();
        Command command = event.getCommand();
        command.timestamp(timestamp.clone());
        event.endOfBatch = endOfBatch;
    }

    private void journal(CommandEvent event) throws Exception {
        Command command = event.getCommand();
        event.lockProvider = new TrackingLockProvider(this.lockProvider);
        event.lockProvider.startAsync().awaitRunning();
        journal.journal(command, new JournalListener(event, indexEngine, journal, command), event.lockProvider);
        if (event.endOfBatch) {
            journal.flush();
        }
    }

    private void complete(CommandEvent event) throws Exception {
        if (!event.completed.isCompletedExceptionally()) {
            event.completed.complete(event.getCommand().onCompletion());
            event.lockProvider.release();
        }
    }

    private <T, C extends Command<T>> void translate(CommandEvent event, long sequence, C command,
                                                     Collection<EntitySubscriber> subscribers,
                                                     CompletableFuture<T> completed) {
        event.setEntitySubscribers(subscribers);
        event.setCommandClass((Class<Command>) command.getClass());
        event.commands.put(command.getClass(), command);
        event.completed = completed;
    }

    @Override
    public <T, C extends Command<T>> CompletableFuture<T> publish(C command, Collection<EntitySubscriber> subscribers) {
        CompletableFuture<T> future = new CompletableFuture<>();
        ringBuffer.publishEvent(this::translate, command, subscribers, future);
        return future;
    }


    @Override @SuppressWarnings("unchecked")
    protected void doStart() {
        log.info("Starting command consumer");

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("eventsourcing-%d").setDaemon(true)
                                                                .build();

        disruptor = new Disruptor<>(() -> new CommandEvent(commandClasses), RING_BUFFER_SIZE, threadFactory);
        disruptor.setDefaultExceptionHandler(new CommandEventExceptionHandler());

        disruptor.handleEventsWith(this::timestamp).thenHandleEventsWithWorkerPool(this::journal)
                 .thenHandleEventsWithWorkerPool(this::complete);

        ringBuffer = disruptor.start();

        notifyStarted();
    }

    @Override
    protected void doStop() {
        disruptor.shutdown();
        notifyStopped();
    }


    private class CommandEventExceptionHandler implements com.lmax.disruptor.ExceptionHandler<CommandEvent> {

        @Override
        public void handleEventException(Throwable ex, long sequence, CommandEvent event) {
            event.lockProvider.release();
            event.completed.completeExceptionally(ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {

        }
    }
}
