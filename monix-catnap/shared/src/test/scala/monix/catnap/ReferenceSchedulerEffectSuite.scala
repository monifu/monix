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
import minitest.SimpleTestSuite
import monix.execution.schedulers.ReferenceSchedulerSuite.DummyScheduler

import scala.concurrent.duration._
import scala.util.Success
import cats.effect.Spawn

class ReferenceSchedulerEffectSuite extends SimpleTestSuite {
  test("clock.monotonic") {
    val s = new DummyScheduler
    val clock = SchedulerEffect.clock[IO](s)

    val clockMonotonic = clock.monotonic(MILLISECONDS).unsafeRunSync()
    assert(clockMonotonic > 0)
  }

  test("clock.realTime") {
    val s = new DummyScheduler
    val clock = SchedulerEffect.clock[IO](s)

    val clockRealTime = clock.realTime(MILLISECONDS).unsafeRunSync()
    assert(clockRealTime > 0)
  }

  test("timer.sleep") {
    val s = new DummyScheduler
    val timer = SchedulerEffect.timerLiftIO[IO](s)

    val f = timer.sleep(10.seconds).unsafeToFuture()
    assertEquals(f.value, None)

    s.tick(5.seconds)
    assertEquals(f.value, None)

    s.tick(5.seconds)
    assertEquals(f.value, Some(Success(())))
  }

  test("contextShift.shift") {
    val s = new DummyScheduler
    val contextShift = SchedulerEffect.contextShift[IO](s)

    val f = Spawn[IO].cede.unsafeToFuture()
    assertEquals(f.value, None)

    s.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("contextShift.evalOn") {
    val s = new DummyScheduler
    val contextShift = SchedulerEffect.contextShift[IO](s)
    val s2 = new DummyScheduler()

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
