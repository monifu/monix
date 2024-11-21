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

import cats.effect._
import cats.laws._
import cats.laws.discipline._
import cats.syntax.all._
import cats.{ effect, Eval }
import monix.catnap.SchedulerEffect
import monix.execution.CancelablePromise
import monix.execution.exceptions.DummyException
import monix.execution.internal.Platform
import org.reactivestreams.{ Publisher, Subscriber, Subscription }

import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object TaskConversionsSuite extends BaseTestSuite {
  test("Task.from(task.to[IO]) == task") { implicit s =>
    check1 { (task: Task[Int]) =>
      Task.from(task.to[IO]) <-> task
    }
  }

  test("Task.from(IO.raiseError(e))") { implicit s =>
    val dummy = DummyException("dummy")
    val task = Task.from(IO.raiseError(dummy))
    assertEquals(task.runToFuture.value, Some(Failure(dummy)))
  }

  test("Task.from(IO.raiseError(e).shift)") { implicit s =>
    val dummy = DummyException("dummy")
    val task = Task.from(for (_ <- IO.shift(s); x <- IO.raiseError[Int](dummy)) yield x)
    val f = task.runToFuture

    assertEquals(f.value, None)
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.now(v).to[IO]") { implicit s =>
    assertEquals(Task.now(10).to[IO].unsafeRunSync(), 10)
  }

  test("Task.raiseError(dummy).to[IO]") { implicit s =>
    val dummy = DummyException("dummy")
    intercept[DummyException] {
      Task.raiseError[Unit](dummy).to[IO].unsafeRunSync()
    }
    ()
  }

  test("Task.eval(thunk).to[IO]") { implicit s =>
    assertEquals(Task.eval(10).to[IO].unsafeRunSync(), 10)
  }

  test("Task.eval(fa).asyncBoundary.to[IO]") { implicit s =>
    val io = Task.eval(1).asyncBoundary.to[IO]
    val f = io.unsafeToFuture()

    assertEquals(f.value, None); s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.raiseError(dummy).asyncBoundary.to[IO]") { implicit s =>
    val dummy = DummyException("dummy")
    val io = Task.raiseError[Int](dummy).executeAsync.to[IO]
    val f = io.unsafeToFuture()

    assertEquals(f.value, None); s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.fromConcurrent(task.toConcurrent[IO]) == task") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    check1 { (task: Task[Int]) =>
      Task.fromConcurrentEffect(task.toConcurrent[IO]) <-> task
    }
  }

  test("Task.fromAsync(task.toAsync[IO]) == task") { implicit s =>
    check1 { (task: Task[Int]) =>
      Task.fromEffect(task.toAsync[IO]) <-> task
    }
  }

  test("Task.fromConcurrent(task) == task") { implicit s =>
    val ref = Task.evalAsync(1)
    assertEquals(Task.fromConcurrentEffect(ref), ref)
  }

  test("Task.fromConcurrent(io)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val f = Task.fromConcurrentEffect(IO(1)).runToFuture
    assertEquals(f.value, Some(Success(1)))

    val io2 = for (_ <- IO.shift; a <- IO(1)) yield a
    val f2 = Task.fromConcurrentEffect(io2).runToFuture
    assertEquals(f2.value, None); s.tick()
    assertEquals(f2.value, Some(Success(1)))
  }

  test("Task.fromAsync(Effect)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val ioEffect: Effect[CIO] = new CustomEffect

    val f = Task.fromEffect(CIO(IO(1))).runToFuture
    assertEquals(f.value, Some(Success(1)))

    val io2 = for (_ <- CIO(IO.shift); a <- CIO(IO(1))) yield a
    val f2 = Task.fromEffect(io2).runToFuture
    assertEquals(f2.value, None); s.tick()
    assertEquals(f2.value, Some(Success(1)))

    val dummy = DummyException("dummy")
    val f3 = Task.fromEffect(CIO(IO.raiseError(dummy))).runToFuture
    assertEquals(f3.value, Some(Failure(dummy)))
  }

  test("Task.fromConcurrent(ConcurrentEffect)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val ioEffect: ConcurrentEffect[CIO] = new CustomConcurrentEffect()

    val f = Task.fromConcurrentEffect(CIO(IO(1))).runToFuture
    assertEquals(f.value, Some(Success(1)))

    val io2 = for (_ <- CIO(IO.shift); a <- CIO(IO(1))) yield a
    val f2 = Task.fromConcurrentEffect(io2).runToFuture
    assertEquals(f2.value, None); s.tick()
    assertEquals(f2.value, Some(Success(1)))

    val dummy = DummyException("dummy")
    val f3 = Task.fromConcurrentEffect(CIO(IO.raiseError(dummy))).runToFuture
    assertEquals(f3.value, Some(Failure(dummy)))
  }

  test("Task.fromAsync(broken Effect)") { implicit s =>
    val dummy = DummyException("dummy")
    implicit val ioEffect: Effect[CIO] =
      new CustomEffect()(IO.contextShift(s)) {
        override def runAsync[A](fa: CIO[A])(cb: (Either[Throwable, A]) => IO[Unit]): SyncIO[Unit] =
          throw dummy
      }

    val f = Task.fromEffect(CIO(IO(1))).runToFuture
    s.tick()

    assertEquals(f.value, None)
    assertEquals(s.state.lastReportedError, dummy)
  }

  test("Task.fromConcurrent(broken ConcurrentEffect)") { implicit s =>
    val dummy = DummyException("dummy")
    implicit val ioEffect: ConcurrentEffect[CIO] =
      new CustomConcurrentEffect()(IO.contextShift(s)) {
        override def runCancelable[A](fa: CIO[A])(cb: Either[Throwable, A] => IO[Unit]): SyncIO[CancelToken[CIO]] =
          throw dummy
      }

    val f = Task.fromConcurrentEffect(CIO(IO(1))).runToFuture
    assertEquals(f.value, None); s.tick()
    assertEquals(f.value, None)

    assertEquals(s.state.lastReportedError, dummy)
  }

  test("Task.from is cancelable") { implicit s =>
    val timer = SchedulerEffect.timerLiftIO[IO](s)
    val io = timer.sleep(10.seconds)
    val f = Task.from(io).runToFuture

    s.tick()
    assert(s.state.tasks.nonEmpty, "tasks.nonEmpty")
    assertEquals(f.value, None)

    f.cancel()
    s.tick()
    assert(s.state.tasks.isEmpty, "tasks.isEmpty")
    assertEquals(f.value, None)

    s.tick(10.seconds)
    assertEquals(f.value, None)
  }

  test("Task.fromConcurrent(io) is cancelable") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val timer = SchedulerEffect.timer[IO](s)
    val io = timer.sleep(10.seconds)
    val f = Task.fromConcurrentEffect(io).runToFuture

    s.tick()
    assert(s.state.tasks.nonEmpty, "tasks.nonEmpty")
    assertEquals(f.value, None)

    f.cancel()
    s.tick()
    assert(s.state.tasks.isEmpty, "tasks.isEmpty")
    assertEquals(f.value, None)

    s.tick(10.seconds)
    assertEquals(f.value, None)
  }

  test("Task.fromConcurrent(ConcurrentEffect) is cancelable") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val effect: ConcurrentEffect[CIO] = new CustomConcurrentEffect

    val timer = SchedulerEffect.timer[CIO](s)
    val io = timer.sleep(10.seconds)
    val f = Task.fromConcurrentEffect(io)(effect).runToFuture

    s.tick()
    assert(s.state.tasks.nonEmpty, "tasks.nonEmpty")
    assertEquals(f.value, None)

    f.cancel()
    assert(s.state.tasks.isEmpty, "tasks.isEmpty")
    assertEquals(f.value, None)

    s.tick(10.seconds)
    assertEquals(f.value, None)
  }

  test("Task.fromConcurrent(task.to[IO]) preserves cancelability") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val task0 = Task(1).delayExecution(10.seconds)
    val task = Task.fromConcurrentEffect(task0.toConcurrent[IO])

    val f = task.runToFuture
    s.tick()
    assertEquals(f.value, None)

    f.cancel()
    s.tick()
    assertEquals(f.value, None)
    assert(s.state.tasks.isEmpty, "tasks.isEmpty")

    s.tick(10.seconds)
    assertEquals(f.value, None)
  }

  test("Task.fromConcurrent(task.to[CIO]) preserves cancelability") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val effect: ConcurrentEffect[CIO] = new CustomConcurrentEffect

    val task0 = Task(1).delayExecution(10.seconds)
    val task = Task.fromConcurrentEffect(task0.toConcurrent[CIO])

    val f = task.runToFuture
    s.tick()
    assertEquals(f.value, None)

    f.cancel()
    s.tick()
    assertEquals(f.value, None)
    assert(s.state.tasks.isEmpty, "tasks.isEmpty")

    s.tick(10.seconds)
    assertEquals(f.value, None)
  }

  test("Task.fromAsync(task.to[IO]) preserves cancelability (because IO is known)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val task0 = Task(1).delayExecution(10.seconds)
    val task = Task.fromEffect(task0.toConcurrent[IO])

    val f = task.runToFuture
    s.tick()
    assertEquals(f.value, None)

    f.cancel()
    s.tick()
    assertEquals(f.value, None)
    assert(s.state.tasks.isEmpty, "tasks.isEmpty")

    s.tick(10.seconds)
    assertEquals(f.value, None)
  }

  test("Task.fromConcurrent(task.toConcurrent[F]) <-> task (Effect)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val effect: CustomConcurrentEffect = new CustomConcurrentEffect

    check1 { (task: Task[Int]) =>
      Task.fromConcurrentEffect(task.toConcurrent[CIO]) <-> task
    }
  }

  test("Task.fromAsync(task.toAsync[F]) <-> task") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val effect: CustomEffect = new CustomEffect

    check1 { (task: Task[Int]) =>
      Task.fromEffect(task.toAsync[CIO]) <-> task
    }
  }

  test("Task.fromConcurrent(task.to[F]) <-> task (ConcurrentEffect)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val effect: CustomConcurrentEffect = new CustomConcurrentEffect

    check1 { (task: Task[Int]) =>
      Task.fromConcurrentEffect(task.toConcurrent[CIO]) <-> task
    }
  }

  test("Task.from[Eval]") { implicit s =>
    var effect = 0
    val task = Task.from(Eval.always { effect += 1; effect })

    assertEquals(task.runToFuture.value, Some(Success(1)))
    assertEquals(task.runToFuture.value, Some(Success(2)))
    assertEquals(task.runToFuture.value, Some(Success(3)))
  }

  test("Task.from[Eval] protects against user error") { implicit s =>
    val dummy = DummyException("dummy")
    val task = Task.from(Eval.always { throw dummy })
    assertEquals(task.runToFuture.value, Some(Failure(dummy)))
  }

  test("Task.fromCancelablePromise") { implicit s =>
    val p = CancelablePromise[Int]()
    val task = Task.fromCancelablePromise(p)

    val token1 = task.runToFuture
    val token2 = task.runToFuture

    token1.cancel()
    p.success(1)

    s.tick()
    assertEquals(token2.value, Some(Success(1)))
    assertEquals(token1.value, None)

    val token3 = task.runToFuture
    assertEquals(token3.value, Some(Success(1)))
  }

  test("Task.fromCancelablePromise stack safety") { implicit s =>
    val count = if (Platform.isJVM) 10000 else 1000

    val p = CancelablePromise[Int]()
    val task = Task.fromCancelablePromise(p)

    def loop(n: Int): Task[Int] =
      if (n > 0) task.flatMap(_ => loop(n - 1))
      else task

    val f = loop(count).runToFuture
    s.tick()
    assertEquals(f.value, None)

    p.success(99)
    s.tick()
    assertEquals(f.value, Some(Success(99)))

    val f2 = loop(count).runToFuture
    s.tick()
    assertEquals(f2.value, Some(Success(99)))
  }

  test("Task.fromReactivePublisher protects against user error") { implicit s =>
    val dummy = DummyException("dummy")

    val pub = new Publisher[Int] {
      def subscribe(s: Subscriber[_ >: Int]): Unit = {
        s.onSubscribe(new Subscription {
          def request(n: Long): Unit = throw dummy
          def cancel(): Unit = throw dummy
        })
      }
    }

    assertEquals(Task.fromReactivePublisher(pub).runToFuture.value, Some(Failure(dummy)))
  }

  test("Task.fromReactivePublisher yields expected input") { implicit s =>
    val pub = new Publisher[Int] {
      def subscribe(s: Subscriber[_ >: Int]): Unit = {
        s.onSubscribe(new Subscription {
          var isActive = true
          def request(n: Long): Unit = {
            if (n > 0 && isActive) {
              isActive = false
              s.onNext(1)
              s.onComplete()
            }
          }
          def cancel(): Unit = {
            isActive = false
          }
        })
      }
    }

    assertEquals(Task.fromReactivePublisher(pub).runToFuture.value, Some(Success(Some(1))))
  }

  test("Task.fromReactivePublisher <-> task") { implicit s =>
    check1 { (task: Task[Int]) =>
      Task.fromReactivePublisher(task.toReactivePublisher) <-> task.map(Some(_))
    }
  }

  final case class CIO[+A](io: IO[A])

  class CustomEffect(implicit cs: ContextShift[IO]) extends Effect[CIO] {
    override def runAsync[A](fa: CIO[A])(cb: (Either[Throwable, A]) => IO[Unit]): SyncIO[Unit] =
      fa.io.runAsync(cb)
    override def async[A](k: ((Either[Throwable, A]) => Unit) => Unit): CIO[A] =
      CIO(IO.async_(k))
    override def asyncF[A](k: ((Either[Throwable, A]) => Unit) => CIO[Unit]): CIO[A] =
      CIO(IO.asyncF(cb => k(cb).io))
    override def suspend[A](thunk: => CIO[A]): CIO[A] =
      CIO(IO.defer(thunk.io))
    override def flatMap[A, B](fa: CIO[A])(f: (A) => CIO[B]): CIO[B] =
      CIO(fa.io.flatMap(a => f(a).io))
    override def tailRecM[A, B](a: A)(f: (A) => CIO[Either[A, B]]): CIO[B] =
      CIO(IO.ioConcurrentEffect.tailRecM(a)(x => f(x).io))
    override def raiseError[A](e: Throwable): CIO[A] =
      CIO(IO.raiseError(e))
    override def handleErrorWith[A](fa: CIO[A])(f: (Throwable) => CIO[A]): CIO[A] =
      CIO(IO.ioConcurrentEffect.handleErrorWith(fa.io)(x => f(x).io))
    override def pure[A](x: A): CIO[A] =
      CIO(IO.pure(x))
    override def liftIO[A](ioa: IO[A]): CIO[A] =
      CIO(ioa)
    override def bracketCase[A, B](acquire: CIO[A])(use: A => CIO[B])(
      release: (A, ExitCase[Throwable]) => CIO[Unit]
    ): CIO[B] =
      CIO(acquire.io.bracketCase(a => use(a).io)((a, e) => release(a, e).io))
  }

  class CustomConcurrentEffect(implicit cs: ContextShift[IO]) extends CustomEffect with ConcurrentEffect[CIO] {

    override def runCancelable[A](fa: CIO[A])(cb: Either[Throwable, A] => IO[Unit]): SyncIO[CancelToken[CIO]] =
      fa.io.runCancelable(cb).map(CIO(_))
    override def cancelable[A](k: (Either[Throwable, A] => Unit) => CancelToken[CIO]): CIO[A] =
      CIO(IO.cancelable(cb => k(cb).io))
    override def uncancelable[A](fa: CIO[A]): CIO[A] =
      CIO(fa.io.uncancelable)
    override def start[A](fa: CIO[A]): CIO[effect.Fiber[CIO, A]] =
      CIO(fa.io.start.map(fiberT))
    override def racePair[A, B](fa: CIO[A], fb: CIO[B]) =
      CIO(IO.racePair(fa.io, fb.io).map {
        case Left((a, fiber)) => Left((a, fiberT(fiber)))
        case Right((fiber, b)) => Right((fiberT(fiber), b))
      })
    private def fiberT[A](fiber: effect.Fiber[IO, A]): effect.Fiber[CIO, A] =
      effect.Fiber(CIO(fiber.join), CIO(fiber.cancel))
  }
}
