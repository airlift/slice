/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.slice.re2;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDfaReaderRegistry
{
    @Test
    public void testPersistentWorkersRegisterOnce()
            throws Exception
    {
        Prog program = Compiler.compile(
                RegexpParser.parse(Slices.utf8Slice("[a-z]+"), Regexp.LIKE_PERL).regexp(),
                false,
                1 << 20);
        Slice input = Slices.utf8Slice("abcdefghijklmnopqrstuvwxyz");
        ThreadLocal<?> readerSlot = readerSlotThreadLocal();

        int workerCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        try {
            CountDownLatch registered = new CountDownLatch(workerCount);
            CountDownLatch release = new CountDownLatch(1);
            List<Future<Object>> registrations = new ArrayList<>();
            for (int worker = 0; worker < workerCount; worker++) {
                registrations.add(executor.submit(() -> {
                    Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
                    Object slot = readerSlot.get();
                    registered.countDown();
                    release.await();
                    return slot;
                }));
            }

            registered.await();
            Dfa.ReaderRegistrySnapshot registeredSnapshot = Dfa.readerRegistrySnapshot();
            assertThat(registeredSnapshot.liveReferences()).isGreaterThanOrEqualTo(workerCount);
            assertThat(registeredSnapshot.activeReaders()).isZero();

            release.countDown();
            Set<Object> registeredSlots = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Future<Object> registration : registrations) {
                registeredSlots.add(registration.get());
            }
            assertThat(registeredSlots).hasSize(workerCount);

            List<Future<Object>> searches = new ArrayList<>();
            for (int search = 0; search < 100; search++) {
                searches.add(executor.submit(() -> {
                    Dfa.search(program, input, false, Prog.MatchKind.FIRST_MATCH, true);
                    return readerSlot.get();
                }));
            }
            for (Future<Object> search : searches) {
                assertThat(registeredSlots).contains(search.get());
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReaderSlotCannotRetainSearchObjects()
    {
        Class<?> readerSlot = nestedClass("ReaderSlot");

        for (Field field : readerSlot.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                assertThat(field.getType().isPrimitive())
                        .as("reader slot field %s must not retain an object", field.getName())
                        .isTrue();
            }
        }
    }

    @Test
    public void testStaleReaderWaveCompactsRegistry()
            throws Exception
    {
        Dfa.drainStaleReaderSlots();
        Object registryBeforeWave = Dfa.readerRegistryRoot();
        int baselineReferences = Dfa.readerRegistrySnapshot().references();
        int staleReferenceCount = Math.max(1_000, baselineReferences + 1);

        addClearedReaderSlots(registryBeforeWave, staleReferenceCount);
        assertThat(Dfa.readerRegistrySnapshot().staleReferences()).isGreaterThanOrEqualTo(staleReferenceCount);

        assertThat(Dfa.drainStaleReaderSlots()).isGreaterThanOrEqualTo(staleReferenceCount);
        assertThat(Dfa.readerRegistrySnapshot().references()).isLessThanOrEqualTo(baselineReferences);
        assertThat(Dfa.readerRegistryRoot()).isNotSameAs(registryBeforeWave);
    }

    @SuppressWarnings("unchecked")
    private static void addClearedReaderSlots(Object registryRoot, int count)
            throws Exception
    {
        Class<?> readerSlotClass = nestedClass("ReaderSlot");
        Class<?> readerSlotReferenceClass = nestedClass("ReaderSlotReference");
        var readerSlotConstructor = readerSlotClass.getDeclaredConstructor();
        var referenceConstructor = readerSlotReferenceClass.getDeclaredConstructor(readerSlotClass);
        readerSlotConstructor.setAccessible(true);
        referenceConstructor.setAccessible(true);

        Set<Object> registry = (Set<Object>) registryRoot;
        for (int referenceIndex = 0; referenceIndex < count; referenceIndex++) {
            Object readerSlot = readerSlotConstructor.newInstance();
            Reference<?> reference = (Reference<?>) referenceConstructor.newInstance(readerSlot);
            registry.add(reference);
            reference.clear();
            assertThat(reference.enqueue()).isTrue();
        }
    }

    private static Class<?> nestedClass(String name)
    {
        for (Class<?> nestedClass : Dfa.class.getDeclaredClasses()) {
            if (nestedClass.getSimpleName().equals(name)) {
                return nestedClass;
            }
        }
        throw new AssertionError("Nested class not found: " + name);
    }

    private static ThreadLocal<?> readerSlotThreadLocal()
            throws Exception
    {
        Field field = Dfa.class.getDeclaredField("THREAD_READER_SLOT");
        field.setAccessible(true);
        return (ThreadLocal<?>) field.get(null);
    }
}
