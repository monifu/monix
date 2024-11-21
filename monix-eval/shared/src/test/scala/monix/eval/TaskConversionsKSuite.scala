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

import scala.util.Success

object TaskConversionsKSuite extends BaseTestSuite {
  test("Task.liftTo[IO]") { implicit s =>
    var effect = 0
    val task = Task { effect += 1; effect }
    val io = Task.liftTo[IO].apply(task)

    assertEquals(io.unsafeRunSync(), 1)
    assertEquals(io.unsafeRunSync(), 2)
  }

  test("Task.liftToAsync[IO]") { implicit s =>
    var effect = 0
    val task = Task { effect += 1; effect }
    val io = Task.liftToAsync[IO].apply(task)

    assertEquals(io.unsafeRunSync(), 1)
    assertEquals(io.unsafeRunSync(), 2)
  }

  test("Task.liftToConcurrent[IO]") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
    var effect = 0
    val task = Task { effect += 1; effect }
    val io = Task.liftToConcurrent[IO].apply(task)

    assertEquals(io.unsafeRunSync(), 1)
    assertEquals(io.unsafeRunSync(), 2)
  }

  test("Task.liftFrom[IO]") { implicit s =>
    var effect = 0
    val io0 = IO { effect += 1; effect }
    val task = Task.liftFrom[IO].apply(io0)

    val f1 = task.runToFuture; s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = task.runToFuture; s.tick()
    assertEquals(f2.value, Some(Success(2)))
  }

  test("Task.liftFromEffect[IO]") { implicit s =>
    var effect = 0
    val io0 = IO { effect += 1; effect }
    val task = Task.liftFromEffect[IO].apply(io0)

    val f1 = task.runToFuture; s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = task.runToFuture; s.tick()
    assertEquals(f2.value, Some(Success(2)))
  }

  test("Task.liftFromConcurrentEffect[IO]") { implicit s =>
    implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](s)(IO.ioEffect)

    var effect = 0
    val io0 = IO { effect += 1; effect }
    val task = Task.liftFromConcurrentEffect[IO].apply(io0)

    val f1 = task.runToFuture; s.tick()
    assertEquals(f1.value, Some(Success(1)))
    val f2 = task.runToFuture; s.tick()
    assertEquals(f2.value, Some(Success(2)))
  }
}
