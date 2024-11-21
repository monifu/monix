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
import cats.effect.IO
import monix.catnap.SchedulerEffect
import monix.execution.exceptions.DummyException

import scala.util.{ Failure, Success }

object TaskLiftSuite extends BaseTestSuite {
  import TaskConversionsSuite.{ CIO, CustomConcurrentEffect, CustomEffect }

  test("task.to[Task]") { _ =>
    val task = Task(1)
    val conv = task.to[Task]
    assertEquals(task, conv)
  }

  test("task.to[IO]") { implicit s =>
    val task = Task(1)
    val io = task.to[IO]
    val f = io.unsafeToFuture()

    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("task.to[IO] for errors") { implicit s =>
    val dummy = DummyException("dummy")
    val task = Task.raiseError(dummy)
    val io = task.to[IO]
    val f = io.unsafeToFuture()

    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("task.to[Effect]") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomEffect = new CustomEffect()

    val task = Task(1)
    val io = task.to[CIO]
    val f = io.io.unsafeToFuture()

    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("task.to[Effect] for errors") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomEffect = new CustomEffect()

    val dummy = DummyException("dummy")
    val task = Task.raiseError(dummy)
    val io = task.to[CIO]
    val f = io.io.unsafeToFuture()

    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }

  test("task.to[ConcurrentEffect]") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomConcurrentEffect = new CustomConcurrentEffect()

    val task = Task(1)
    val io = task.to[CIO]
    val f = io.io.unsafeToFuture()

    s.tick()
    assertEquals(f.value, Some(Success(1)))
  }

  test("task.to[ConcurrentEffect] for errors") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    implicit val F: CustomConcurrentEffect = new CustomConcurrentEffect()

    val dummy = DummyException("dummy")
    val task = Task.raiseError(dummy)
    val io = task.to[CIO]
    val f = io.io.unsafeToFuture()

    s.tick()
    assertEquals(f.value, Some(Failure(dummy)))
  }
}
