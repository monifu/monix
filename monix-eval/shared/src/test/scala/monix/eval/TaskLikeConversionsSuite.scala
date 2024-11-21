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

import cats.Eval
import cats.effect.{ IO, SyncIO }
import monix.catnap.SchedulerEffect
import monix.execution.CancelablePromise
import monix.execution.exceptions.DummyException

import scala.concurrent.Promise
import scala.util.{ Failure, Success, Try }

object TaskLikeConversionsSuite extends BaseTestSuite {
  import TaskConversionsSuite.{ CIO, CustomConcurrentEffect, CustomEffect }

  test("Task.from(future)") { implicit s =>
    val p = Promise[Int]()
    val f = Task.from(p.future).runToFuture

    s.tick()
    assertEquals(f.value, None)

    p.success(1)
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.from(future) for errors") { implicit s =>
    val p = Promise[Int]()
    val dummy = DummyException("dummy")
    val f = Task.from(p.future).runToFuture

    s.tick()
    assertEquals(f.value, None)

    p.failure(dummy)
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.from(IO)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val p = Promise[Int]()
    val f = Task.from(IO.fromFuture(IO.pure(p.future))).runToFuture

    s.tick()
    assertEquals(f.value, None)

    p.success(1)
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.from(IO) for errors") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    val p = Promise[Int]()
    val dummy = DummyException("dummy")
    val f = Task.from(IO.fromFuture(IO.pure(p.future))).runToFuture

    s.tick()
    assertEquals(f.value, None)

    p.failure(dummy)
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.from(Task)") { _ =>
    val source = Task(1)
    val conv = Task.from(source)
    assertEquals(source, conv)
  }

  test("Task.from(Coeval)") { implicit s =>
    var effect = false
    val source = Coeval { effect = true; 1 }
    val conv = Task.from(source)
    assert(!effect)

    val f = conv.runToFuture
    s.tick()
    assertEquals(f.value, Some(Success(1)))
    assert(effect)
  }

  test("Task.from(Coeval) for errors") { implicit s =>
    var effect = false
    val dummy = DummyException("dummy")
    val source = Coeval[Int] { effect = true; throw dummy }
    val conv = Task.from(source)
    assert(!effect)

    val f = conv.runToFuture
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
    assert(effect)
  }

  test("Task.from(Eval)") { implicit s =>
    var effect = false
    val source = Eval.always { effect = true; 1 }
    val conv = Task.from(source)
    assert(!effect)

    val f = conv.runToFuture
    s.tick()
    assertEquals(f.value, Some(Success(1)))
    assert(effect)
  }

  test("Task.from(Eval) for errors") { implicit s =>
    var effect = false
    val dummy = DummyException("dummy")
    val source = Eval.always[Int] { effect = true; throw dummy }
    val conv = Task.from(source)
    assert(!effect)

    val f = conv.runToFuture
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
    assert(effect)
  }

  test("Task.from(SyncIO)") { implicit s =>
    var effect = false
    val source = SyncIO { effect = true; 1 }
    val conv = Task.from(source)
    assert(!effect)

    val f = conv.runToFuture
    s.tick()
    assertEquals(f.value, Some(Success(1)))
    assert(effect)
  }

  test("Task.from(SyncIO) for errors") { implicit s =>
    var effect = false
    val dummy = DummyException("dummy")
    val source = SyncIO.defer[Int] { effect = true; SyncIO.raiseError(dummy) }
    val conv = Task.from(source)
    assert(!effect)

    val f = conv.runToFuture
    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
    assert(effect)
  }

  test("Task.from(Try)") { implicit s =>
    val source = Success(1): Try[Int]
    val conv = Task.from(source)
    assertEquals(conv.runToFuture.value, Some(Success(1)))
  }

  test("Task.from(Try) for errors") { implicit s =>
    val dummy = DummyException("dummy")
    val source = Failure(dummy): Try[Int]
    val conv = Task.from(source)
    assertEquals(conv.runToFuture.value, Some(Failure(dummy)))
  }

  test("Task.from(Either)") { implicit s =>
    val source: Either[Throwable, Int] = Right(1)
    val conv = Task.from(source)
    assertEquals(conv.runToFuture.value, Some(Success(1)))
  }

  test("Task.from(Either) for errors") { implicit s =>
    val dummy = DummyException("dummy")
    val source: Either[Throwable, Int] = Left(dummy)
    val conv = Task.from(source)
    assertEquals(conv.runToFuture.value, Some(Failure(dummy)))
  }

  test("Task.from(custom Effect)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomEffect = new CustomEffect()

    var effect = false
    val source = CIO(IO { effect = true; 1 })
    val conv = Task.from(source)

    assert(!effect)
    val f = conv.runToFuture
    s.tick()
    assert(effect)
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.from(custom Effect) for errors") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomEffect = new CustomEffect()

    var effect = false
    val dummy = DummyException("dummy")
    val source = CIO(IO { effect = true; throw dummy })
    val conv = Task.from(source)

    assert(!effect)
    val f = conv.runToFuture
    s.tick()
    assert(effect)
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.from(custom ConcurrentEffect)") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomConcurrentEffect = new CustomConcurrentEffect()(cs)

    var effect = false
    val source = CIO(IO { effect = true; 1 })
    val conv = Task.from(source)

    assert(!effect)
    val f = conv.runToFuture
    s.tick()
    assert(effect)
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.from(custom ConcurrentEffect) for errors") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomConcurrentEffect = new CustomConcurrentEffect()(cs)

    var effect = false
    val dummy = DummyException("dummy")
    val source = CIO(IO { effect = true; throw dummy })
    val conv = Task.from(source)

    assert(!effect)
    val f = conv.runToFuture
    s.tick()
    assert(effect)
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("Task.from(Function0)") { implicit s =>
    val task = Task.from(() => 1)
    val f = task.runToFuture
    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("Task.from(CancelablePromise)") { implicit s =>
    val p = CancelablePromise[Int]()
    val task = Task.from(p)

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
}
