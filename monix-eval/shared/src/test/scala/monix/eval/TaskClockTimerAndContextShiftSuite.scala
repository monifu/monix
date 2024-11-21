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

package monix.eval

import java.util.concurrent.TimeUnit

import cats.effect.Clock
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler

import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import cats.effect.Temporal

object TaskClockTimerAndContextShiftSuite extends BaseTestSuite {
  test("Task.clock is implicit") { _ =>
    assertEquals(Task.clock, implicitly[Clock[Task]])
  }

  test("Task.clock.monotonic") { implicit s =>
    s.tick(1.seconds)

    val f = Task.clock.monotonic(TimeUnit.SECONDS).runToFuture
    s.tick()

    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.clock.realTime") { implicit s =>
    s.tick(1.seconds)

    val f = Task.clock.realTime(TimeUnit.SECONDS).runToFuture
    s.tick()

    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.clock(s2).monotonic") { implicit s =>
    val s2 = TestScheduler()
    s2.tick(1.seconds)

    val f = Task.clock(s2).monotonic(TimeUnit.SECONDS).runToFuture
    s.tick()

    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.clock(s).realTime") { implicit s =>
    val s2 = TestScheduler()
    s2.tick(1.seconds)

    val f = Task.clock(s2).realTime(TimeUnit.SECONDS).runToFuture
    s.tick()

    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.timer is implicit") { _ =>
    assertEquals(Task.timer, implicitly[Temporal[Task]])
    assertEquals(Task.timer.clock, implicitly[Clock[Task]])
  }

  test("Task.timer") { implicit s =>
    val f = Task.timer.sleep(1.second).runToFuture
    s.tick()
    assertEquals(f.value, None)
    s.tick(1.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("Task.timer(s)") { implicit s =>
    val s2 = TestScheduler()
    val f = Task.timer(s2).sleep(1.second).runToFuture

    s.tick()
    assertEquals(f.value, None)
    s.tick(1.second)
    assertEquals(f.value, None)
    s2.tick(1.second)
    s.tick(1.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("Task.timer(s).clock") { implicit s =>
    val s2 = TestScheduler()
    s2.tick(1.second)

    val f = Task.timer(s2).clock.monotonic(TimeUnit.SECONDS).runToFuture
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.contextShift is implicit") { _ =>
    assertEquals(Task.contextShift, implicitly[ContextShift[Task]])
  }

  test("Task.contextShift.shift") { implicit s =>
    val f = Task.contextShift.shift.runToFuture
    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("Task.contextShift.evalOn(s2)") { implicit s =>
    val s2 = TestScheduler()
    val f = Task.contextShift.evalOn(s2)(Task(1)).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.contextShift.evalOn(s2) failure") { implicit s =>
    val s2 = TestScheduler()
    val dummy = DummyException("dummy")
    val f = Task.contextShift.evalOn(s2)(Task.raiseError(dummy)).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.contextShift.evalOn(s2) uses s2 for async boundaries") { implicit s =>
    val s2 = TestScheduler()
    val f = Task.contextShift.evalOn(s2)(Task(1).delayExecution(100.millis)).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    s.tick(100.millis)
    assertEquals(f.value, None)
    s2.tick(100.millis)
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.contextShift.evalOn(s2) injects s2 to Task.deferAction") { implicit s =>
    val s2 = TestScheduler()

    var wasScheduled = false
    val runnable: Runnable = () => wasScheduled = true

    val f = Task.contextShift.evalOn(s2)(Task.deferAction(scheduler => Task(scheduler.execute(runnable)))).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    assertEquals(wasScheduled, true)
    s.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("Task.contextShift(s).shift") { implicit s =>
    val f = Task.contextShift(s).shift.runToFuture
    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("Task.contextShift(s).evalOn(s2)") { implicit s =>
    val s2 = TestScheduler()
    val f = Task.contextShift(s).evalOn(s2)(Task(1)).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.contextShift(s).evalOn(s2) failure") { implicit s =>
    val s2 = TestScheduler()
    val dummy = DummyException("dummy")
    val f = Task.contextShift(s).evalOn(s2)(Task.raiseError(dummy)).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.contextShift(s).evalOn(s2) uses s2 for async boundaries") { implicit s =>
    val s2 = TestScheduler()
    val f = Task.contextShift(s).evalOn(s2)(Task(1).delayExecution(100.millis)).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    s.tick(100.millis)
    assertEquals(f.value, None)
    s2.tick(100.millis)
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.contextShift(s).evalOn(s2) injects s2 to Task.deferAction") { implicit s =>
    val s2 = TestScheduler()
    var wasScheduled = false
    val runnable: Runnable = () => wasScheduled = true

    val f =
      Task.contextShift(s).evalOn(s2)(
        Task.deferAction { scheduler =>
          Task(scheduler.execute(runnable))
        }
      ).runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, None)
    s2.tick()
    assertEquals(wasScheduled, true)
    s.tick()
    assertEquals(f.value, Some(Success(())))
  }
}
