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

import monix.execution.exceptions.DummyException

import concurrent.duration._
import scala.util.{ Failure, Success }

class TaskSequenceSuite extends BaseTestSuite {
  fixture.test("Task.sequence should not execute in parallel") { implicit s =>
    val seq = Seq(
      Task.evalAsync(1).delayExecution(2.seconds),
      Task.evalAsync(2).delayExecution(1.second),
      Task.evalAsync(3).delayExecution(3.seconds)
    )
    val f = Task.sequence(seq).runToFuture

    s.tick()
    assertEquals(f.value, None)
    s.tick(2.seconds)
    assertEquals(f.value, None)
    s.tick(1.second)
    assertEquals(f.value, None)
    s.tick(3.second)
    assertEquals(f.value, Some(Success(Seq(1, 2, 3))))
  }

  fixture.test("Task.sequence should onError if one of the tasks terminates in error") { implicit s =>
    val ex = DummyException("dummy")
    val seq = Seq(
      Task.evalAsync(2).delayExecution(1.second),
      Task.evalAsync(throw ex).delayExecution(2.seconds),
      Task.evalAsync(3).delayExecution(3.seconds),
      Task.evalAsync(3).delayExecution(1.seconds)
    )

    val f = Task.sequence(seq).runToFuture

    // First
    s.tick(1.second)
    assertEquals(f.value, None)
    // Second
    s.tick(2.second)
    assertEquals(f.value, Some(Failure(ex)))
  }

  fixture.test("Task.sequence should be canceled") { implicit s =>
    val seq = Seq(
      Task.evalAsync(1).delayExecution(2.seconds),
      Task.evalAsync(2).delayExecution(1.second),
      Task.evalAsync(3).delayExecution(3.seconds)
    )
    val f = Task.sequence(seq).runToFuture

    s.tick()
    assertEquals(f.value, None)
    s.tick(2.seconds)
    assertEquals(f.value, None)

    f.cancel()
    s.tick(1.second)
    assertEquals(f.value, None)
  }
}
