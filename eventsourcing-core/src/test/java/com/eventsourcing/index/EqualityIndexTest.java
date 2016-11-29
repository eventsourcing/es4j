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
import com.eventsourcing.models.Car;
import com.eventsourcing.models.CarFactory;
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.AttributeIndex;
import com.googlecode.cqengine.index.support.CloseableIterable;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.index.support.KeyStatistics;
import com.googlecode.cqengine.index.support.KeyStatisticsIndex;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.googlecode.cqengine.query.QueryFactory.*;
import static com.googlecode.cqengine.stream.StreamFactory.streamOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// This test was originally copied from CQEngine in order to test
// indices the same way CQEngine does.
public abstract class EqualityIndexTest<HashIndex extends AttributeIndex> {

    public <O extends Entity> IndexedCollection<EntityHandle<O>> createIndexedCollection(Class<O> klass) {
        return new ConcurrentIndexedCollection<>();
    }

    public abstract <A, O extends Entity> HashIndex onAttribute(Attribute<O, A> attribute);

    public static <A> Set<A> setOf(A... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    public static <A> Set<A> setOf(CloseableIterable<A> values) {
        Set<A> result = new LinkedHashSet<>();
        CloseableIterator<A> iterator = values.iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    @Test
    public void getDistinctKeysAndCounts() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MODEL_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MODEL);
        MODEL_INDEX.clear(noQueryOptions());

        collection.addIndex(MODEL_INDEX);

        collection.addAll(CarFactory.createCollectionOfCars(20));

        Set<String> distinctModels = setOf(MODEL_INDEX.getDistinctKeys(noQueryOptions()));
        assertEquals(distinctModels,
                     setOf("Accord", "Avensis", "Civic", "Focus", "Fusion", "Hilux", "Insight", "M6", "Prius",
                           "Taurus"));
        for (String model : distinctModels) {
            assertEquals(MODEL_INDEX.getCountForKey(model, noQueryOptions()), Integer.valueOf(2));
        }

        MODEL_INDEX.clear(noQueryOptions());
    }

    @Test
    public void getCountOfDistinctKeys() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);
        MANUFACTURER_INDEX.clear(noQueryOptions());

        collection.addIndex(MANUFACTURER_INDEX);

        collection.addAll(CarFactory.createCollectionOfCars(20));

        assertEquals(MANUFACTURER_INDEX.getCountOfDistinctKeys(noQueryOptions()), Integer.valueOf(4));

        MANUFACTURER_INDEX.clear(noQueryOptions());
    }

    @Test
    public void getStatisticsForDistinctKeys() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);
        MANUFACTURER_INDEX.clear(noQueryOptions());

        collection.addIndex(MANUFACTURER_INDEX);

        collection.addAll(CarFactory.createCollectionOfCars(20));

        Set<KeyStatistics<String>> keyStatistics = setOf(
                MANUFACTURER_INDEX.getStatisticsForDistinctKeys(noQueryOptions()));
        assertEquals(keyStatistics, setOf(
                new KeyStatistics<>("Ford", 6),
                new KeyStatistics<>("Honda", 6),
                new KeyStatistics<>("Toyota", 6),
                new KeyStatistics<>("BMW", 2)
                     )
        );
        MANUFACTURER_INDEX.clear(noQueryOptions());
    }

    @Test
    public void retrieveEqual() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);
        MANUFACTURER_INDEX.clear(noQueryOptions());

        collection.addIndex(MANUFACTURER_INDEX);

        collection.addAll(CarFactory.createCollectionOfCars(10));

        ResultSet<EntityHandle<Car>> cars = collection.retrieve(equal(Car.MANUFACTURER, "Honda"));
        assertTrue(cars.isNotEmpty());
        assertTrue(streamOf(cars).allMatch(car -> car.get().getManufacturer().contentEquals("Honda")));

        cars.close();
        MANUFACTURER_INDEX.clear(noQueryOptions());
    }

    @Test
    public void byteArrayEquality() {
        IndexedCollection<EntityHandle<Car>> collection = createIndexedCollection(Car.class);
        SimpleAttribute<Car, byte[]> attr = new SimpleAttribute<Car, byte[]>(Car.class,
                                                                            (Class<EntityHandle<Car>>) ((Class<?>)getClass()),
                                                                            byte[].class, "bytearray") {

            @Override public byte[] getValue(Car object, QueryOptions queryOptions) {
                return object.getManufacturer().getBytes();
            }
        };
        HashIndex index = onAttribute(attr);
        index.clear(noQueryOptions());

        collection.addIndex(index);

        collection.addAll(CarFactory.createCollectionOfCars(10));

        ResultSet<EntityHandle<Car>> cars = collection.retrieve(equal(attr, "Honda".getBytes()));
        assertTrue(cars.isNotEmpty());
        assertTrue(streamOf(cars).allMatch(car -> car.get().getManufacturer().contentEquals("Honda")));

        cars.close();
        index.clear(noQueryOptions());
    }

    @Test
    public void retrieveHas() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);
        MANUFACTURER_INDEX.clear(noQueryOptions());
        collection.addIndex(MANUFACTURER_INDEX);

        Set<EntityHandle<Car>> coll = CarFactory.createCollectionOfCars(10);
        collection.addAll(coll);

        ResultSet<EntityHandle<Car>> cars = collection.retrieve(has(Car.MANUFACTURER));
        assertTrue(cars.isNotEmpty());
        assertEquals(cars.size(), coll.size());

        cars.close();
        MANUFACTURER_INDEX.clear(noQueryOptions());
    }

    @Test
    public void indexExistingData() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);
        MANUFACTURER_INDEX.clear(noQueryOptions());

        collection.addAll(CarFactory.createCollectionOfCars(10));

        collection.addIndex(MANUFACTURER_INDEX);

        ResultSet<EntityHandle<Car>> cars = collection.retrieve(equal(Car.MANUFACTURER, "Honda"));
        assertTrue(cars.isNotEmpty());
        assertTrue(streamOf(cars).allMatch(car -> car.get().getManufacturer().contentEquals("Honda")));

        cars.close();
        MANUFACTURER_INDEX.clear(noQueryOptions());
    }

    @Test
    public void reindexData() {
        IndexedCollection<EntityHandle<Car>> collection = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);
        MANUFACTURER_INDEX.clear(noQueryOptions());

        Set<EntityHandle<Car>> cars = CarFactory.createCollectionOfCars(10);

        collection.addAll(cars);

        collection.addIndex(MANUFACTURER_INDEX);

        IndexedCollection<EntityHandle<Car>> collection1 = new ConcurrentIndexedCollection<>();
        KeyStatisticsIndex<String, EntityHandle<Car>> MANUFACTURER_INDEX1 =
                (KeyStatisticsIndex<String, EntityHandle<Car>>) onAttribute(Car.MANUFACTURER);

        collection1.addAll(cars);

        collection1.addIndex(MANUFACTURER_INDEX1);


        assertEquals((int)MANUFACTURER_INDEX.getCountForKey("Honda", noQueryOptions()), 3);
        assertEquals((int)MANUFACTURER_INDEX1.getCountForKey("Honda", noQueryOptions()), 3);

        MANUFACTURER_INDEX.clear(noQueryOptions());
        MANUFACTURER_INDEX1.clear(noQueryOptions());
    }
}
