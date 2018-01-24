/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
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

package monix.tail

import cats.effect.IO
import cats.laws._
import cats.laws.discipline._
import monix.execution.exceptions.DummyException
import monix.tail.BracketResult._
import monix.tail.batches.{Batch, BatchCursor}

object IterantBracketSuite extends BaseTestSuite {
  class Resource(var acquired: Int = 0, var released: Int = 0) {
    def acquire = IO { acquired += 1 }
    def release = IO { released += 1 }
  }

  def runIterant[A](iterant: Iterant[IO, A]): Unit =
    iterant.completeL.unsafeRunSync()

  test("Bracket yields all elements `use` provides") { _ =>
    check1 { (source: Iterant[IO, Int]) =>
      val bracketed = Iterant.bracket(IO.unit)(
        _ => source,
        (_, _) => IO.unit
      )

      source <-> bracketed
    }
  }

  test("Bracket preserves earlyStop of stream returned from `use`") { _ =>
    var earlyStopDone = false
    val bracketed = Iterant.bracket(IO.unit)(
      _ => Iterant[IO].of(1, 2, 3).doOnEarlyStop(IO {
        earlyStopDone = true
      }),
      (_, _) => IO.unit
    )
    runIterant(bracketed.take(1))
    assert(earlyStopDone)
  }

  test("Bracket releases resource on normal completion") { _ =>
    val rs = new Resource
    val bracketed = Iterant.bracket(rs.acquire)(
      _ => Iterant.range(1, 10),
      (_, result) => rs.release.flatMap(_ => IO {
        assertEquals(result, Completed)
      })
    )
    runIterant(bracketed)
    assertEquals(rs.acquired, 1)
    assertEquals(rs.released, 1)
  }

  test("Bracket releases resource on early stop") { _ =>
    val rs = new Resource
    val bracketed = Iterant.bracket(rs.acquire)(
      _ => Iterant.range(1, 10),
      (_, result) => rs.release.flatMap(_ => IO {
        assertEquals(result, EarlyStop)
      })
    ).take(1)
    runIterant(bracketed)
    assertEquals(rs.acquired, 1)
    assertEquals(rs.released, 1)
  }

  test("Bracket releases resource on exception") { _ =>
    val rs = new Resource
    val error = DummyException("dummy")
    val bracketed = Iterant.bracket(rs.acquire)(
      _ => Iterant.range[IO](1, 10) ++ Iterant.raiseError[IO, Int](error),
      (_, result) => rs.release.flatMap(_ => IO {
        assertEquals(result, Error(error))
      })
    )
    intercept[DummyException] {
      runIterant(bracketed)
    }
    assertEquals(rs.acquired, 1)
    assertEquals(rs.released, 1)
  }

  test("Bracket releases resource if `use` throws") { _ =>
    val rs = new Resource
    val dummy = DummyException("dummy")
    val bracketed = Iterant.bracket(rs.acquire)(
      _ => throw dummy,
      (_, result) => rs.release.flatMap(_ => IO {
        assertEquals(result, Error(dummy))
      })
    )
    intercept[DummyException] {
      runIterant(bracketed)
    }
    assertEquals(rs.acquired, 1)
    assertEquals(rs.released, 1)
  }

  test("Bracket does not call `release` if `acquire` has an error") { _ =>
    val rs = new Resource
    val dummy = DummyException("dummy")
    val bracketed = Iterant.bracket(
      IO.raiseError(dummy).flatMap(_ => rs.acquire))(
      _ => Iterant.empty[IO, Int],
      (_, result) => rs.release.flatMap(_ => IO {
        assertEquals(result, Error(dummy))
      })
    )
    intercept[DummyException] {
      runIterant(bracketed)
    }
    assertEquals(rs.acquired, 0)
    assertEquals(rs.released, 0)
  }

  test("Bracket nesting: outer releases even if inner release fails") { _ =>
    var released = false
    val dummy = DummyException("dummy")
    val bracketed = Iterant.bracket(IO.unit)(
      _ => Iterant.bracket(IO.unit)(
        _ => Iterant[IO].of(1, 2, 3),
        (_, _) => IO.raiseError(dummy)
      ),
      (_, _) => IO { released = true }
    )

    intercept[DummyException] {
      runIterant(bracketed)
    }
    assert(released)
  }

  test("Bracket nesting: inner releases even if outer release fails") { _ =>
    var released = false
    val dummy = DummyException("dummy")
    val bracketed = Iterant.bracket(IO.unit)(
      _ => Iterant.bracket(IO.unit)(
        _ => Iterant[IO].of(1, 2, 3),
        (_, _) => IO { released = true }
      ),
      (_, _) => IO.raiseError(dummy)
    )

    intercept[DummyException] {
      runIterant(bracketed)
    }
    assert(released)
  }

  // TODO: requires exception handling in completeL
/*  test("Bracket handles broken batches & cursors") { _ =>
    val rs = new Resource
    val dummy = DummyException("dummy")
    val brokenBatch = Iterant.bracket(rs.acquire)(
      _ => Iterant.nextBatchS(
        ThrowExceptionBatch(dummy),
        IO(Iterant.empty[IO, Int]),
        IO.unit
      ),
      (_,_) => rs.release
    )
    val brokenCursor = Iterant.bracket(rs.acquire)(
      _ => Iterant.nextCursorS(
        ThrowExceptionCursor(dummy),
        IO(Iterant.empty[IO, Int]),
        IO.unit
      ),
      (_,_) => rs.release
    )
    intercept[DummyException] {
      brokenBatch.completeL.unsafeRunSync()
    }
    intercept[DummyException] {
      brokenCursor.completeL.unsafeRunSync()
    }
    assertEquals(rs.acquired, 2)
    assertEquals(rs.released, 2)
  }*/

  test("Bracket handles broken `next` continuations") { _ =>
    val rs = new Resource
    val dummy = DummyException("dummy")
    def withError(ctor: (IO[Iterant[IO, Int]], IO[Unit]) => Iterant[IO, Int]) =
      Iterant.bracket(rs.acquire)(
        _ => ctor(IO.raiseError(dummy), IO.unit),
        (_, _) => rs.release
      )

    val brokens = Array(
      withError(Iterant.nextS(0, _, _)),
      withError(Iterant.nextBatchS(Batch(1, 2, 3), _, _)),
      withError(Iterant.nextCursorS(BatchCursor(1, 2, 3), _, _)),
      withError(Iterant.suspendS)
    )

    for (broken <- brokens) {
      intercept[DummyException] {
        runIterant(broken)
      }
    }
    assertEquals(rs.acquired, brokens.length)
    assertEquals(rs.released, brokens.length)
  }
}
