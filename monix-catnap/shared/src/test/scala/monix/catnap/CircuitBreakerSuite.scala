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

import cats.effect._
import cats.implicits._
import minitest.TestSuite
import monix.catnap.CircuitBreaker.{ Closed, Open }
import monix.execution.exceptions.{ DummyException, ExecutionRejectedException }
import monix.execution.schedulers.TestScheduler

import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import cats.effect.Temporal

object CircuitBreakerSuite extends TestSuite[TestScheduler] {
  def setup() = TestScheduler()
  def tearDown(env: TestScheduler): Unit =
    assert(env.state.tasks.isEmpty, "There should be no tasks left!")

  implicit def timer(implicit ec: TestScheduler): Temporal[IO] =
    SchedulerEffect.timerLiftIO[IO](ec)

  implicit def contextShift(implicit ec: TestScheduler): ContextShift[IO] =
    SchedulerEffect.contextShift[IO](ec)(IO.ioEffect)

  test("should work for successful async tasks") { implicit s =>
    val circuitBreaker = CircuitBreaker.unsafe[IO](
      maxFailures = 5,
      resetTimeout = 1.minute
    )

    var effect = 0
    val task = circuitBreaker.protect(IO.shift *> IO {
      effect += 1
    })

    for (_ <- 0 until 10000) task.unsafeToFuture()
    s.tick()
    assertEquals(effect, 10000)
  }

  test("should work for successful immediate tasks") { implicit s =>
    val circuitBreaker = CircuitBreaker.unsafe[IO](
      maxFailures = 5,
      resetTimeout = 1.minute
    )

    var effect = 0
    val task = circuitBreaker.protect(IO {
      effect += 1
    })

    for (_ <- 0 until 10000) task.unsafeToFuture()
    assertEquals(effect, 10000)
  }

  test("should be stack safe for successful async tasks (flatMap)") { implicit s =>
    val circuitBreaker = CircuitBreaker.unsafe[IO](
      maxFailures = 5,
      resetTimeout = 1.minute
    )

    def loop(n: Int, acc: Int): IO[Int] = {
      if (n > 0)
        circuitBreaker
          .protect(IO.shift *> IO(acc + 1))
          .flatMap(s => loop(n - 1, s))
      else
        IO.pure(acc)
    }

    val f = loop(100000, 0).unsafeToFuture(); s.tick()
    assertEquals(f.value, Some(Success(100000)))
  }

  test("should be stack safe for successful async tasks (inner protect calls)") { implicit s =>
    val circuitBreaker = CircuitBreaker
      .of[IO](
        maxFailures = 5,
        resetTimeout = 1.minute
      )
      .unsafeRunSync()

    def loop(n: Int, acc: Int): IO[Int] =
      IO.shift *> IO.defer {
        if (n > 0)
          circuitBreaker.protect(loop(n - 1, acc + 1))
        else
          IO.pure(acc)
      }

    val f = loop(100000, 0).unsafeToFuture(); s.tick()
    assertEquals(f.value, Some(Success(100000)))
  }

  test("should be stack safe for successful immediate tasks (flatMap)") { implicit s =>
    val circuitBreaker = CircuitBreaker
      .of[IO](
        maxFailures = 5,
        resetTimeout = 1.minute
      )
      .unsafeRunSync()

    def loop(n: Int, acc: Int): IO[Int] = {
      if (n > 0)
        circuitBreaker
          .protect(IO(acc + 1))
          .flatMap(s => loop(n - 1, s))
      else
        IO.pure(acc)
    }

    val f = loop(100000, 0).unsafeToFuture(); s.tick()
    assertEquals(f.value, Some(Success(100000)))
  }

  test("should be stack safe for successful immediate tasks (defer)") { implicit s =>
    val circuitBreaker = CircuitBreaker
      .of[IO](
        maxFailures = 5,
        resetTimeout = 1.minute
      )
      .unsafeRunSync()

    def loop(n: Int, acc: Int): IO[Int] =
      IO.defer {
        if (n > 0)
          circuitBreaker.protect(loop(n - 1, acc + 1))
        else
          IO.pure(acc)
      }

    val f = loop(100000, 0).unsafeToFuture(); s.tick()
    assertEquals(f.value, Some(Success(100000)))
  }

  test("complete workflow with failures and exponential backoff") { implicit s =>
    var openedCount = 0
    var closedCount = 0
    var halfOpenCount = 0
    var rejectedCount = 0

    val circuitBreaker = {
      val cb = CircuitBreaker
        .of[IO](
          maxFailures = 5,
          resetTimeout = 1.minute,
          exponentialBackoffFactor = 2,
          maxResetTimeout = 10.minutes
        )
        .unsafeRunSync()

      cb.doOnOpen(IO { openedCount += 1 })
        .doOnClosed(IO { closedCount += 1 })
        .doOnHalfOpen(IO { halfOpenCount += 1 })
        .doOnRejectedTask(IO { rejectedCount += 1 })
    }

    val dummy = DummyException("dummy")
    val taskInError = circuitBreaker.protect(IO[Int](throw dummy))
    val taskSuccess = circuitBreaker.protect(IO { 1 })

    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    assertEquals(circuitBreaker.state.unsafeRunSync(), CircuitBreaker.Closed(2))

    // A successful value should reset the counter
    assertEquals(taskSuccess.unsafeToFuture().value, Some(Success(1)))
    assertEquals(circuitBreaker.state.unsafeRunSync(), CircuitBreaker.Closed(0))

    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    assertEquals(circuitBreaker.state.unsafeRunSync(), CircuitBreaker.Closed(4))

    assertEquals(taskInError.unsafeToFuture().value, Some(Failure(dummy)))
    circuitBreaker.state.unsafeRunSync() match {
      case CircuitBreaker.Open(sa, rt) =>
        assertEquals(sa, s.clockMonotonic(MILLISECONDS))
        assertEquals(rt, 1.minute)
      case other =>
        fail(s"Invalid state: $other")
    }

    // Getting rejections from now on, testing reset timeout
    var resetTimeout = 60.seconds
    for (_ <- 0 until 30) {
      val now = s.clockMonotonic(MILLISECONDS)
      val nextTimeout = {
        val value = resetTimeout * 2.toLong
        if (value > 10.minutes) 10.minutes else value
      }

      intercept[ExecutionRejectedException] {
        taskInError.unsafeToFuture().value.get.get
        ()
      }

      s.tick(resetTimeout - 1.second)

      intercept[ExecutionRejectedException] {
        taskInError.unsafeToFuture().value.get.get
        ()
      }

      // After 1 minute we should attempt a reset
      s.tick(1.second)
      circuitBreaker.state.unsafeRunSync() match {
        case CircuitBreaker.Open(sa, rt) =>
          assertEquals(sa, now)
          assertEquals(rt, resetTimeout)
        case other =>
          fail(s"Invalid state: $other")
      }

      // Starting the HalfOpen state
      val delayedTask = circuitBreaker.protect(IO.sleep(1.second) *> IO.raiseError(dummy))
      val delayedResult = delayedTask.unsafeToFuture()

      circuitBreaker.state.unsafeRunSync() match {
        case CircuitBreaker.HalfOpen(rt) =>
          assertEquals(rt, resetTimeout)
        case other =>
          fail(s"Invalid state: $other")
      }

      // Rejecting all other tasks
      intercept[ExecutionRejectedException] {
        taskInError.unsafeToFuture().value.get.get
        ()
      }
      intercept[ExecutionRejectedException] {
        taskInError.unsafeToFuture().value.get.get
        ()
      }

      // Should migrate back into Open
      s.tick(1.second)
      assertEquals(delayedResult.value, Some(Failure(dummy)))
      circuitBreaker.state.unsafeRunSync() match {
        case CircuitBreaker.Open(sa, rt) =>
          assertEquals(sa, s.clockMonotonic(MILLISECONDS))
          assertEquals(rt, nextTimeout)
        case other =>
          fail(s"Invalid state: $other")
      }

      intercept[ExecutionRejectedException] {
        taskInError.unsafeToFuture().value.get.get
        ()
      }

      // Calculate next reset timeout
      resetTimeout = nextTimeout
    }

    // Going back into Closed
    s.tick(resetTimeout)

    val delayedTask = circuitBreaker.protect(IO.sleep(1.second) *> IO(1))
    val delayedResult = delayedTask.unsafeToFuture()

    circuitBreaker.state.unsafeRunSync() match {
      case CircuitBreaker.HalfOpen(rt) =>
        assertEquals(rt, resetTimeout)
      case other =>
        fail(s"Invalid state: $other")
    }

    intercept[ExecutionRejectedException] {
      taskInError.unsafeToFuture().value.get.get
      ()
    }

    s.tick(1.second)
    assertEquals(delayedResult.value, Some(Success(1)))
    assertEquals(circuitBreaker.state.unsafeRunSync(), CircuitBreaker.Closed(0))

    assertEquals(rejectedCount, 5 * 30 + 1)
    assertEquals(openedCount, 30 + 1)
    assertEquals(halfOpenCount, 30 + 1)
    assertEquals(closedCount, 1)
  }

  test("validate parameters") { implicit s =>
    intercept[IllegalArgumentException] {
      // Positive maxFailures
      CircuitBreaker.unsafe[IO](
        maxFailures = -1,
        resetTimeout = 1.minute
      )
      ()
    }

    intercept[IllegalArgumentException] {
      // Strictly positive resetTimeout
      CircuitBreaker.unsafe[IO](
        maxFailures = 2,
        resetTimeout = -1.minute
      )
      ()
    }

    intercept[IllegalArgumentException] {
      // exponentialBackoffFactor >= 1
      CircuitBreaker.unsafe[IO](
        maxFailures = 2,
        resetTimeout = 1.minute,
        exponentialBackoffFactor = 0.5
      )
      ()
    }

    intercept[IllegalArgumentException] {
      // Strictly positive maxResetTimeout
      CircuitBreaker.unsafe[IO](
        maxFailures = 2,
        resetTimeout = 1.minute,
        exponentialBackoffFactor = 2,
        maxResetTimeout = Duration.Zero
      )
      ()
    }
    ()
  }

  test("awaitClose") { implicit s =>
    val cb = CircuitBreaker.unsafe[IO](1, 1.second)

    val dummy = DummyException("dummy")
    val f = cb.protect(IO.raiseError(dummy)).unsafeToFuture()
    assertEquals(f.value, Some(Failure(dummy)))

    cb.state.unsafeRunSync() match {
      case Open(_, _) => ()
      case other => fail(s"Invalid state: $other")
    }

    val onClose = cb.awaitClose.unsafeToFuture(); s.tick()
    assertEquals(onClose.value, None)

    val f2 = cb.protect(IO(1)).unsafeToFuture()
    f2.value match {
      case Some(Failure(_: ExecutionRejectedException)) => ()
      case other => fail(s"Unexpected result: $other")
    }

    s.tick(1.second)
    val f3 = cb.protect(IO.sleep(1.second) *> IO(1)).unsafeToFuture()
    s.tick()

    assertEquals(onClose.value, None)
    assertEquals(f3.value, None)

    val f4 = cb.protect(IO(1)).unsafeToFuture()
    f4.value match {
      case Some(Failure(_: ExecutionRejectedException)) => ()
      case other => fail(s"Unexpected result: $other")
    }

    s.tick(1.second)
    assertEquals(onClose.value, Some(Success(())))
    assertEquals(f3.value, Some(Success(1)))
  }

  test("works with Sync only") { implicit s =>
    implicit val clock: Clock[SyncIO] = SchedulerEffect.clock[SyncIO](s)
    val cb = CircuitBreaker.unsafe[SyncIO](1, 1.second)

    val dummy = DummyException("dummy")
    val f = cb.protect(SyncIO.raiseError(dummy)).attempt.unsafeRunSync()
    assertEquals(f, Left(dummy))

    cb.state.unsafeRunSync() match {
      case Open(_, _) => ()
      case other => fail(s"Invalid state: $other")
    }

    val f2 = cb.protect(SyncIO(1)).attempt.unsafeRunSync()
    f2 match {
      case Left(_: ExecutionRejectedException) => ()
      case other => fail(s"Unexpected result: $other")
    }

    s.tick(1.second)
    val f3 = cb.protect(SyncIO(1)).attempt.unsafeRunSync()
    assertEquals(f3, Right(1))
  }

  test("awaitClose with Sync instance override") { implicit s =>
    // Trying to override the Sync[IO] instance.
    import Overrides.syncIO
    assertEquals(Sync[IO], syncIO)

    val cb = CircuitBreaker.unsafe[IO](1, 1.second)
    awaitCloseSuccessfulTest(s, cb)
  }

  test("awaitClose with polymorphic code") { implicit s =>
    // Forcing the use of Sync[IO]
    def mkInstance[F[_]](implicit F: Sync[F], clock: Clock[F]) =
      CircuitBreaker.unsafe[F](1, 1.second)

    val cb = mkInstance[IO]
    awaitCloseSuccessfulTest(s, cb)
  }

  private def awaitCloseSuccessfulTest(s: TestScheduler, cb: CircuitBreaker[IO]) = {
    cb.protect(IO.raiseError(DummyException("dummy"))).unsafeToFuture()
    s.tick()

    cb.state.unsafeRunSync() match {
      case CircuitBreaker.Open(_, _) => ()
      case other => fail(s"Unexpected state: $other")
    }

    val f = cb.awaitClose.unsafeToFuture()
    s.tick()
    assertEquals(f.value, None)

    s.tick(1.second)
    cb.protect(IO(1)).unsafeToFuture()
    s.tick()

    assertEquals(cb.state.unsafeRunSync(), CircuitBreaker.Closed(0))
    assertEquals(f.value, Some(Success(())))
  }

  test("awaitClose works when instance was built with Sync[F]") { implicit s =>
    // Overriding Sync[IO]
    import Overrides.syncIO
    // Forcing the use of Sync[IO]
    def mkInstance[F[_]](implicit F: Sync[F], clock: Clock[F]) =
      CircuitBreaker.unsafe[F](1, 1.second)

    val cb = mkInstance[IO]
    cb.protect(IO.raiseError(DummyException("dummy"))).unsafeToFuture()
    s.tick()

    cb.state.unsafeRunSync() match {
      case CircuitBreaker.Open(_, _) => ()
      case other => fail(s"Unexpected state: $other")
    }

    val f = cb.awaitClose.unsafeToFuture()
    s.tick()
    assertEquals(f.value, None)

    s.tick(1.second)
    cb.protect(IO(1)).unsafeToFuture()
    s.tick()

    assertEquals(cb.state.unsafeRunSync(), CircuitBreaker.Closed(0))
    assertEquals(f.value, Some(Success(())))
  }

  test("canceled tasks in half-open state should open with old timeout") { implicit s =>
    import scala.concurrent.duration._

    val scenario = for {
      cb     <- CircuitBreaker.of[IO](0, 5.second)
      _      <- cb.protect(IO.raiseError(DummyException("boom"))).attempt
      _      <- IO.sleep(5.second)
      state1 <- cb.state
      _      <- cb.protect(IO.sleep(2.second)).timeoutTo(1.second, IO.unit)
      state2 <- cb.state
      _      <- cb.protect(IO.unit)
      state3 <- cb.state
    } yield {
      state1 match {
        case Open(startedAt, resetTimeout) =>
          assertEquals(startedAt, 0L)
          assertEquals(resetTimeout, 5.second)
        case _ =>
          fail("CircuitBreaker should be in Open state")
      }

      state2 match {
        case Open(startedAt, resetTimeout) =>
          assertEquals(startedAt, 0L)
          assertEquals(resetTimeout, 5.second)
        case _ =>
          fail("CircuitBreaker should be in Open state")
      }

      assertEquals(state3, Closed(0))
    }

    val f = scenario.unsafeToFuture()
    s.tick(1.day)
    assertEquals(f.value, Some(Success(())))
  }
}
