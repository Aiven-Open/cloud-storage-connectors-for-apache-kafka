/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;

class AbstractSourceTaskTest {

    @Test
    void timerTest() {
        final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(Duration.ofSeconds(1));
        assertThat(timer.millisecondsRemaining()).isEqualTo(Duration.ofSeconds(1).toMillis());
        timer.start();
        await().atMost(Duration.ofSeconds(2)).until(timer::isExpired);
        assertThat(timer.millisecondsRemaining()).isLessThan(0);
        timer.stop();
        assertThat(timer.millisecondsRemaining()).isEqualTo(Duration.ofSeconds(1).toMillis());
    }

    @Test
    void timerSequenceTest() {
        final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(Duration.ofSeconds(1));
        // stopped state does not allow stop
        assertThatExceptionOfType(IllegalStateException.class).as("stop while not running")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
        timer.reset(); // verify that an exception is not thrown.

        // started state does not allow start
        timer.start();
        assertThatExceptionOfType(IllegalStateException.class).as("start while running")
                .isThrownBy(timer::start)
                .withMessageStartingWith("Timer: ");
        timer.reset();
        timer.start(); // restart the timer.
        timer.stop();

        // stopped state does not allow stop or start
        assertThatExceptionOfType(IllegalStateException.class).as("stop after stop")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
        assertThatExceptionOfType(IllegalStateException.class).as("start after stop")
                .isThrownBy(timer::start)
                .withMessageStartingWith("Timer: ");
        timer.reset();

        // stopped + reset does not allow stop.
        assertThatExceptionOfType(IllegalStateException.class).as("stop after reset (1)")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
        timer.start();
        timer.reset();

        // started + reset does not allow stop;
        assertThatExceptionOfType(IllegalStateException.class).as("stop after reset (2)")
                .isThrownBy(timer::stop)
                .withMessageStartingWith("Timer: ");
    }

    @Test
    void backoffTest() throws InterruptedException {
        final AbstractSourceTask.Timer timer = new AbstractSourceTask.Timer(Duration.ofSeconds(1));
        final AbstractSourceTask.Backoff backoff = new AbstractSourceTask.Backoff(timer.getBackoffConfig());
        final long estimatedDelay = backoff.estimatedDelay();
        assertThat(estimatedDelay).isLessThan(500);

        // execute delay without timer running.
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        backoff.delay();
        stopWatch.stop();
        assertThat(stopWatch.getTime()).as("Result without timer running")
                .isBetween(estimatedDelay - backoff.getMaxJitter(), estimatedDelay + backoff.getMaxJitter());

        timer.start();
        for (int i = 0; i < 9; i++) {
            stopWatch.reset();
            timer.reset();
            timer.start();
            stopWatch.start();
            await().atMost(Duration.ofSeconds(2)).until(() -> {
                backoff.delay();
                return backoff.estimatedDelay() == 0 || timer.isExpired();
            });
            stopWatch.stop();
            timer.stop();
            final int step = i;
            if (!timer.isExpired()) {
                assertThat(stopWatch.getTime()).as(() -> String.format("Result with timer running at step %s", step))
                        .isBetween(Duration.ofSeconds(1).toMillis() - backoff.getMaxJitter(),
                                Duration.ofSeconds(1).toMillis() + backoff.getMaxJitter());
            }
        }
    }

    @Test
    void backoffIncrementalTimeTest() throws InterruptedException {
        final AtomicBoolean abortTrigger = new AtomicBoolean();
        // delay increases in powers of 2.
        final long maxDelay = 1000; // not a power of 2
        final AbstractSourceTask.BackoffConfig config = new AbstractSourceTask.BackoffConfig() {
            @Override
            public AbstractSourceTask.SupplierOfLong getSupplierOfTimeRemaining() {
                return () -> maxDelay;
            }

            @Override
            public AbstractSourceTask.AbortTrigger getAbortTrigger() {
                return () -> abortTrigger.set(true);
            }
        };

        final AbstractSourceTask.Backoff backoff = new AbstractSourceTask.Backoff(config);
        long expected = 2;
        while (backoff.estimatedDelay() < maxDelay) {
            assertThat(backoff.estimatedDelay()).isEqualTo(expected);
            backoff.delay();
            expected *= 2;
            assertThat(abortTrigger).isFalse();
        }
        assertThat(backoff.estimatedDelay()).isEqualTo(maxDelay);
    }
}
