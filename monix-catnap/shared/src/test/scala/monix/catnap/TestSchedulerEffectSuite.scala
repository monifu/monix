/*
 * Copyright (c) 2014-2022 Monix Contributors.
 * See the project homepage at: https://monix.io
 *
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

package monix.catnap

import cats.effect.IO
import minitest.TestSuite
import monix.execution.schedulers.TestScheduler

import scala.concurrent.duration._
import scala.util.Success
import cats.effect.Spawn

object TestSchedulerEffectSuite extends TestSuite[TestScheduler] {
  def setup() = TestScheduler()
  def tearDown(env: TestScheduler): Unit = {
    assert(env.state.tasks.isEmpty)
  }

  test("clock.monotonic") { s =>
    val clock = SchedulerEffect.clock[IO](s)
    val fetch = clock.monotonic(MILLISECONDS)

    assertEquals(fetch.unsafeRunSync(), 0L)
    s.tick(5.seconds)
    assertEquals(fetch.unsafeRunSync(), 5000L)
    s.tick(5.seconds)
    assertEquals(fetch.unsafeRunSync(), 10000L)
    s.tick(300.millis)
    assertEquals(fetch.unsafeRunSync(), 10300L)
  }

  test("clock.realTime") { s =>
    val clock = SchedulerEffect.clock[IO](s)
    val fetch = clock.realTime(MILLISECONDS)

    assertEquals(fetch.unsafeRunSync(), 0L)
    s.tick(5.seconds)
    assertEquals(fetch.unsafeRunSync(), 5000L)
    s.tick(5.seconds)
    assertEquals(fetch.unsafeRunSync(), 10000L)
    s.tick(300.millis)
    assertEquals(fetch.unsafeRunSync(), 10300L)
  }

  test("timerLiftIO[IO]") { s =>
    val timer = SchedulerEffect.timerLiftIO[IO](s)
    val clockMono = timer.clock.monotonic(MILLISECONDS)
    val clockReal = timer.clock.realTime(MILLISECONDS)

    val f = timer.sleep(10.seconds).unsafeToFuture()
    assertEquals(f.value, None)

    assertEquals(clockMono.unsafeRunSync(), 0)
    assertEquals(clockReal.unsafeRunSync(), 0)

    s.tick(5.seconds)
    assertEquals(f.value, None)

    assertEquals(clockMono.unsafeRunSync(), 5000)
    assertEquals(clockReal.unsafeRunSync(), 5000)

    s.tick(5.seconds)
    assertEquals(f.value, Some(Success(())))

    assertEquals(clockMono.unsafeRunSync(), 10000)
    assertEquals(clockReal.unsafeRunSync(), 10000)
  }

  test("timer[IO]") { s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val timer = SchedulerEffect.timer[IO](s)
    val clockMono = timer.clock.monotonic(MILLISECONDS)
    val clockReal = timer.clock.realTime(MILLISECONDS)

    val f = timer.sleep(10.seconds).unsafeToFuture()
    assertEquals(f.value, None)

    assertEquals(clockMono.unsafeRunSync(), 0)
    assertEquals(clockReal.unsafeRunSync(), 0)

    s.tick(5.seconds)
    assertEquals(f.value, None)

    assertEquals(clockMono.unsafeRunSync(), 5000)
    assertEquals(clockReal.unsafeRunSync(), 5000)

    s.tick(5.seconds)
    assertEquals(f.value, Some(Success(())))

    assertEquals(clockMono.unsafeRunSync(), 10000)
    assertEquals(clockReal.unsafeRunSync(), 10000)
  }

  test("contextShift.shift") { s =>
    val contextShift = SchedulerEffect.contextShift[IO](s)

    val f = Spawn[IO].cede.unsafeToFuture()
    assertEquals(f.value, None)

    s.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("contextShift.evalOn") { s =>
    val contextShift = SchedulerEffect.contextShift[IO](s)
    val s2 = TestScheduler()

    val f = contextShift.evalOn(s2)(IO(1)).unsafeToFuture()
    assertEquals(f.value, None)

    s.tick()
    assertEquals(f.value, None)

    s2.tick()
    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }
}
