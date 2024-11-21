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

import cats.effect.{ Async, IO }
import minitest.TestSuite
import monix.catnap.syntax._
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler
import monix.execution.{ Cancelable, CancelableFuture }
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success }

object FutureLiftSuite extends TestSuite[TestScheduler] {
  def setup() = TestScheduler()
  def tearDown(env: TestScheduler): Unit =
    assert(env.state.tasks.isEmpty, "There should be no tasks left!")

  implicit def contextShift(implicit ec: TestScheduler): ContextShift[IO] =
    SchedulerEffect.contextShift[IO](ec)(IO.ioEffect)

  test("IO(future).futureLift") { implicit s =>
    var effect = 0
    val io = IO(Future { effect += 1; effect }).futureLift

    val f1 = io.unsafeToFuture(); s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = io.unsafeToFuture(); s.tick()
    assertEquals(f2.value, Some(Success(2)))
  }

  test("IO(Future.successful).futureLift") { implicit s =>
    val io = IO(Future.successful(1)).futureLift

    val f1 = io.unsafeToFuture(); s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = io.unsafeToFuture(); s.tick()
    assertEquals(f2.value, Some(Success(1)))
  }

  test("IO(Future.failed).futureLift") { implicit s =>
    val dummy = DummyException("dummy")
    val io = IO(Future.failed[Int](dummy)).futureLift

    val f1 = io.unsafeToFuture(); s.tick()
    assertEquals(f1.value, Some(Failure(dummy)))
    val f2 = io.unsafeToFuture(); s.tick()
    assertEquals(f2.value, Some(Failure(dummy)))
  }

  test("F.delay(future).futureLift for Async[F] data types") { implicit s =>
    import Overrides.asyncIO
    var effect = 0

    def mkInstance[F[_]: Async] =
      Async[F].delay(Future { effect += 1; effect }).futureLift

    val io = mkInstance[IO]
    val f1 = io.unsafeToFuture(); s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = io.unsafeToFuture(); s.tick()
    assertEquals(f2.value, Some(Success(2)))
  }

  test("F.delay(Future.successful).futureLift for Async[F] data types") { implicit s =>
    import Overrides.asyncIO

    def mkInstance[F[_]: Async] =
      Async[F].delay(Future.successful(1)).futureLift

    val io = mkInstance[IO]
    val f1 = io.unsafeToFuture(); s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = io.unsafeToFuture(); s.tick()
    assertEquals(f2.value, Some(Success(1)))
  }

  test("F.delay(Future.failed).futureLift for Async[F] data types") { implicit s =>
    import Overrides.asyncIO

    val dummy = DummyException("dummy")
    def mkInstance[F[_]: Async] =
      Async[F].delay(Future.failed[Int](dummy)).futureLift

    val io = mkInstance[IO]
    val f1 = io.unsafeToFuture(); s.tick()
    assertEquals(f1.value, Some(Failure(dummy)))
    val f2 = io.unsafeToFuture(); s.tick()
    assertEquals(f2.value, Some(Failure(dummy)))
  }

  test("F.delay(future).futureLift for Concurrent[F] data types") { implicit s =>
    var wasCanceled = 0
    val io = IO(CancelableFuture[Int](
      CancelableFuture.never,
      Cancelable { () =>
        wasCanceled += 1
      }
    )).futureLift

    val p = Promise[Int]()
    val token = io.unsafeRunCancelable {
      case Left(e) => p.failure(e); ()
      case Right(a) => p.success(a); ()
    }

    // Cancelling
    token.unsafeRunAsyncAndForget(); s.tick()
    assertEquals(wasCanceled, 1)
  }

  test("FutureLift[F] instance for Concurrent[F] data types") { implicit s =>
    var wasCanceled = 0
    val source = Promise[Int]()
    val io = FutureLift[IO, CancelableFuture].apply(
      IO(
        CancelableFuture[Int](
          source.future,
          Cancelable { () =>
            wasCanceled += 1
          }
        )
      )
    )

    val p = Promise[Int]()
    val token = io.unsafeRunCancelable {
      case Left(e) => p.failure(e); ()
      case Right(a) => p.success(a); ()
    }

    // Cancelling
    token.unsafeRunAsyncAndForget(); s.tick()
    assertEquals(wasCanceled, 1)
    assertEquals(p.future.value, None)

    val f2 = io.unsafeToFuture()
    source.success(1)
    s.tick()

    assertEquals(f2.value, Some(Success(1)))
  }

  test("FutureLift[F] instance for Async[F] data types") { implicit s =>
    import Overrides.asyncIO

    var wasCanceled = 0
    val source = Promise[Int]()

    def mkInstance[F[_]](implicit F: Async[F]): F[Int] =
      FutureLift[F, CancelableFuture].apply(
        F.delay(
          CancelableFuture[Int](
            source.future,
            Cancelable { () =>
              wasCanceled += 1
            }
          )
        )
      )

    val io = mkInstance[IO]
    val p = Promise[Int]()
    val token = io.unsafeRunCancelable {
      case Left(e) => p.failure(e); ()
      case Right(a) => p.success(a); ()
    }

    // Cancelling
    token.unsafeRunAsyncAndForget(); s.tick()
    assertEquals(wasCanceled, 0)
    assertEquals(p.future.value, None)

    val f2 = io.unsafeToFuture()
    source.success(1)
    s.tick()

    assertEquals(f2.value, Some(Success(1)))
  }
}
