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

import cats.laws._
import cats.laws.discipline._
import cats.syntax.all._
import monix.execution.exceptions.{ CompositeException, DummyException }
import monix.execution.internal.Platform
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import cats.effect.Deferred

object TaskBracketSuite extends BaseTestSuite {
  test("equivalence with onErrorHandleWith") { implicit sc =>
    check2 { (task: Task[Int], f: Throwable => Task[Unit]) =>
      val expected = task.onErrorHandleWith(e => f(e) *> Task.raiseError(e))
      val received = task.bracketE(Task.now) {
        case (_, Left(Some(e))) => f(e)
        case (_, _) => Task.unit
      }
      received <-> expected
    }
  }

  test("equivalence with flatMap + transformWith") { implicit sc =>
    check3 { (acquire: Task[Int], f: Int => Task[Int], release: Int => Task[Unit]) =>
      val expected = acquire.flatMap { a =>
        f(a).redeemWith(
          e => release(a) *> Task.raiseError(e),
          s => release(a) *> Task.pure(s)
        )
      }

      val received = acquire.bracket(f)(release)
      received <-> expected
    }
  }

  test("use is protected against user error") { implicit sc =>
    val dummy = new DummyException("dummy")
    var input = Option.empty[(Int, Either[Option[Throwable], Int])]

    val task = Task.evalAsync(1).bracketE(_ => throw dummy) { (a, i: Either[Option[Throwable], Int]) =>
      Task.eval { input = Some((a, i)) }
    }

    val f = task.runToFuture
    sc.tick()

    assertEquals(input, Some((1, Left(Some(dummy)))))
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("release is evaluated on success") { implicit sc =>
    var input = Option.empty[(Int, Either[Option[Throwable], Int])]
    val task = Task.evalAsync(1).bracketE(x => Task.evalAsync(x + 1)) { (a, i) =>
      Task.eval { input = Some((a, i)) }
    }

    val f = task.runToFuture
    sc.tick()

    assertEquals(input, Some((1, Right(2))))
    assertEquals(f.value, Some(Success(2)))
  }

  test("release is evaluated on error") { implicit sc =>
    val dummy = new DummyException("dummy")
    var input = Option.empty[(Int, Either[Option[Throwable], Int])]

    val task = Task.evalAsync(1).bracketE(_ => Task.raiseError[Int](dummy)) { (a, i) =>
      Task.eval { input = Some((a, i)) }
    }

    val f = task.runToFuture
    sc.tick()

    assertEquals(input, Some((1, Left(Some(dummy)))))
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("release is evaluated on cancel") { implicit sc =>
    import scala.concurrent.duration._
    var input = Option.empty[(Int, Either[Option[Throwable], Int])]

    val task = Task
      .evalAsync(1)
      .bracketE(x => Task.evalAsync(x + 1).delayExecution(1.second)) { (a, i) =>
        Task.eval { input = Some((a, i)) }
      }

    val f = task.runToFuture
    sc.tick()

    f.cancel()
    sc.tick(1.second)

    assertEquals(f.value, None)
    assertEquals(input, Some((1, Left(None))))
  }

  test("if both use and release throw, report release error, signal use error") { implicit sc =>
    val useError = new DummyException("use")
    val releaseError = new DummyException("release")

    val task = Task
      .evalAsync(1)
      .bracket[Int] { _ =>
        Task.raiseError(useError)
      } { _ =>
        Task.raiseError(releaseError)
      }

    val f = task.runToFuture
    sc.tick()

    f.value match {
      case Some(Failure(error)) =>
        if (Platform.isJVM) {
          assertEquals(error, useError)
          error.getSuppressed match {
            case Array(error2) =>
              assertEquals(error2, releaseError)
            case _ =>
              fail("Unexpected suppressed errors list: " + error.getSuppressed.toList)
          }
        } else
          error match {
            case CompositeException(Seq(`useError`, `releaseError`)) =>
              () // pass
            case _ =>
              fail(s"Unexpected error: $error")
          }

      case other =>
        fail(s"Unexpected result: $other")
    }
  }

  test("bracket works with auto-cancelable run-loops") { implicit sc =>
    import concurrent.duration._

    var effect = 0
    val task = Task(1)
      .bracket(_ => Task.sleep(1.second))(_ => Task(effect += 1))
      .executeWithOptions(_.enableAutoCancelableRunLoops)

    val f = task.runToFuture
    sc.tick()
    assertEquals(f.value, None)

    f.cancel()
    assertEquals(f.value, None)
    assertEquals(effect, 1)
  }

  test("bracket is stack safe (1)") { implicit sc =>
    def loop(n: Int): Task[Unit] =
      if (n > 0)
        Task(n).bracket(n => Task(n - 1))(_ => Task.unit).flatMap(loop)
      else
        Task.unit

    val cycles = if (Platform.isJVM) 100000 else 1000
    val f = loop(cycles).runToFuture

    sc.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("bracket is stack safe (2)") { implicit sc =>
    val cycles = if (Platform.isJVM) 100000 else 1000
    val bracket = Task.unit.bracket(_ => Task.unit)(_ => Task.unit)
    val task = (0 until cycles).foldLeft(Task.unit) { (acc, _) =>
      acc.flatMap(_ => bracket)
    }

    val f = task.runToFuture
    sc.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("bracket is stack safe (3)") { implicit sc =>
    val cycles = if (Platform.isJVM) 100000 else 1000
    val task = (0 until cycles).foldLeft(Task.unit) { (acc, _) =>
      acc.bracket(_ => Task.unit)(_ => Task.unit)
    }

    val f = task.runToFuture
    sc.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("bracket is stack safe (4)") { implicit sc =>
    val cycles = if (Platform.isJVM) 100000 else 1000
    val task = (0 until cycles).foldLeft(Task.unit) { (acc, _) =>
      Task.unit.bracket(_ => acc)(_ => Task.unit)
    }

    val f = task.runToFuture
    sc.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("use is not evaluated on cancel") { implicit sc =>
    import scala.concurrent.duration._
    var use = false
    var release = false

    val task = Task
      .sleep(2.second)
      .bracket(_ => Task { use = true })(_ => Task { release = true })

    val f = task.runToFuture
    sc.tick()

    f.cancel()
    sc.tick(2.second)

    assertEquals(f.value, None)
    assertEquals(use, false)
    assertEquals(release, true)
  }

  test("cancel should wait for already started finalizers on success") { implicit sc =>

    val fa = for {
      pa    <- Deferred[Task, Unit]
      fiber <- Task.unit.guarantee(pa.complete(()) >> Task.sleep(1.second)).start
      _     <- pa.get
      _     <- fiber.cancel
    } yield ()

    val f = fa.runToFuture

    sc.tick()
    assertEquals(f.value, None)

    sc.tick(1.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("cancel should wait for already started finalizers on failure") { implicit sc =>
    val dummy = new RuntimeException("dummy")

    val fa = for {
      pa    <- Deferred[Task, Unit]
      fiber <- Task.unit.guarantee(pa.complete(()) >> Task.sleep(1.second) >> Task.raiseError(dummy)).start
      _     <- pa.get
      _     <- fiber.cancel
    } yield ()

    val f = fa.runToFuture

    sc.tick()
    assertEquals(f.value, None)

    sc.tick(1.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("cancel should wait for already started use finalizers") { implicit sc =>

    val fa = for {
      pa <- Deferred[Task, Unit]
      fibA <- Task.unit
        .bracket(_ => Task.unit.guarantee(pa.complete(()) >> Task.sleep(2.second)))(_ => Task.unit)
        .start
      _ <- pa.get
      _ <- fibA.cancel
    } yield ()

    val f = fa.runToFuture

    sc.tick()
    assertEquals(f.value, None)

    sc.tick(2.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("second cancel should wait for use finalizers") { implicit sc =>

    val fa = for {
      pa <- Deferred[Task, Unit]
      fiber <- Task.unit
        .bracket(_ => (pa.complete(()) >> Task.never).guarantee(Task.sleep(2.second)))(_ => Task.unit)
        .start
      _ <- pa.get
      _ <- Task.race(fiber.cancel, fiber.cancel)
    } yield ()

    val f = fa.runToFuture

    sc.tick()
    assertEquals(f.value, None)

    sc.tick(2.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("second cancel during acquire should wait for it and finalizers to complete") { implicit sc =>

    val fa = for {
      pa <- Deferred[Task, Unit]
      fiber <- (pa.complete(()) >> Task.sleep(1.second))
        .bracket(_ => Task.unit)(_ => Task.sleep(1.second))
        .start
      _ <- pa.get
      _ <- Task.race(fiber.cancel, fiber.cancel)
    } yield ()

    val f = fa.runToFuture

    sc.tick()
    assertEquals(f.value, None)

    sc.tick(1.second)
    assertEquals(f.value, None)

    sc.tick(1.second)
    assertEquals(f.value, Some(Success(())))
  }

  test("second cancel during acquire should wait for it and finalizers to complete (non-terminating)") {
    implicit sc =>

      val fa = for {
        pa <- Deferred[Task, Unit]
        fiber <- (pa.complete(()) >> Task.sleep(1.second))
          .bracket(_ => Task.unit)(_ => Task.never)
          .start
        _ <- pa.get
        _ <- Task.race(fiber.cancel, fiber.cancel)
      } yield ()

      val f = fa.runToFuture

      sc.tick()
      assertEquals(f.value, None)

      sc.tick(1.day)
      assertEquals(f.value, None)
  }

  test("Multiple cancel should not hang") { implicit sc =>

    val fa = for {
      fiber <- Task.sleep(1.second).start
      _     <- fiber.cancel
      _     <- fiber.cancel
    } yield ()

    val f = fa.runToFuture

    sc.tick()
    assertEquals(f.value, Some(Success(())))
  }

  test("bracket can be canceled while failing to acquire") { implicit sc =>

    val task = (Task.sleep(2.second) *> Task.raiseError[Unit](DummyException("BOOM")))
      .bracket(_ => Task.unit)(_ => Task.unit)

    val cancelToken = task.runAsyncF(_ => ())

    sc.tick(1.second)
    val f = cancelToken.runToFuture

    sc.tick(1.second)
    assertEquals(f.value, Some(Success(())))
  }
}
