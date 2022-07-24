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

package monix.reactive.observers

import monix.execution.BaseTestSuite

import monix.eval.Coeval
import monix.execution.Ack.{ Continue, Stop }
import monix.execution.atomic.AtomicLong
import monix.execution.internal.{ Platform, RunnableAction }
import monix.execution.{ Ack, Scheduler }
import monix.reactive.Observer
import monix.reactive.OverflowStrategy.ClearBufferAndSignal
import monix.execution.exceptions.DummyException

import scala.concurrent.{ Future, Promise }
object OverflowStrategyClearBufferAndSignalSuite {
  def buildNewWithSignal(bufferSize: Int, underlying: Observer[Int])(implicit s: Scheduler) = {

    BufferedSubscriber(Subscriber(underlying, s), ClearBufferAndSignal(bufferSize, nr => Coeval(Some(nr.toInt))))
  }

  def buildNewWithLog(bufferSize: Int, underlying: Observer[Int], log: AtomicLong)(implicit s: Scheduler) = {

    BufferedSubscriber[Int](
      Subscriber(underlying, s),
      ClearBufferAndSignal(bufferSize, nr => Coeval { log.set(nr); None })
    )
  }
}

class OverflowStrategyClearBufferAndSignalSuite extends BaseTestSuite {
  import OverflowStrategyClearBufferAndSignalSuite._

  fixture.test("should not lose events, test 1") { implicit s =>
    var number = 0
    var wasCompleted = false

    val underlying = new Observer[Int] {
      def onNext(elem: Int): Future[Ack] = {
        number += 1
        Continue
      }

      def onError(ex: Throwable): Unit = {
        s.reportFailure(ex)
      }

      def onComplete(): Unit = {
        wasCompleted = true
      }
    }

    val buffer = buildNewWithSignal(1000, underlying)
    for (i <- 0 until 1000) buffer.onNext(i)
    buffer.onComplete()

    assert(!wasCompleted)
    s.tick()
    assertEquals(number, 1000)
    assert(wasCompleted)
  }

  fixture.test("should not lose events, test 2") { implicit s =>
    var number = 0
    var completed = false

    val underlying = new Observer[Int] {
      def onNext(elem: Int): Future[Ack] = {
        number += 1
        Continue
      }

      def onError(ex: Throwable): Unit = {
        s.reportFailure(ex)
      }

      def onComplete(): Unit = {
        completed = true
      }
    }

    val buffer = buildNewWithSignal(1000, underlying)

    def loop(n: Int): Unit =
      if (n > 0)
        s.execute(RunnableAction { buffer.onNext(n); loop(n - 1) })
      else
        buffer.onComplete()

    loop(10000)
    assert(!completed)
    assertEquals(number, 0)

    s.tick()
    assert(completed)
    assertEquals(number, 10000)
  }

  fixture.test("should drop old events when over capacity and signal") { implicit s =>
    var received = 0
    var wasCompleted = false
    val promise = Promise[Ack]()

    val underlying = new Observer[Int] {
      def onNext(elem: Int) = {
        received += elem
        if (elem < 7) Continue else promise.future
      }

      def onError(ex: Throwable) = ()

      def onComplete() = {
        wasCompleted = true
      }
    }

    val buffer = buildNewWithSignal(8, underlying)

    for (i <- 1 to 7) assertEquals(buffer.onNext(i), Continue)
    s.tick()
    assertEquals(received, 28)

    for (i <- 0 to 2004) assertEquals(buffer.onNext(i), Continue)
    s.tick(); assertEquals(received, 28)

    promise.success(Continue)
    s.tick()

    if (Platform.isJVM)
      assertEquals(received, 28 + (2000 to 2004).sum + 2000)
    else
      assertEquals(received, 28 + (1995 to 2004).sum + 1995)

    buffer.onComplete(); s.tick()
    assert(wasCompleted, "wasCompleted should be true")
  }

  fixture.test("should drop old events when over capacity and log") { implicit s =>
    var received = 0
    var wasCompleted = false
    val promise = Promise[Ack]()

    val underlying = new Observer[Int] {
      def onNext(elem: Int) = {
        received += elem
        if (elem < 7) Continue else promise.future
      }

      def onError(ex: Throwable) = ()

      def onComplete() = {
        wasCompleted = true
      }
    }

    val log = AtomicLong(0)
    val buffer = buildNewWithLog(5, underlying, log)

    for (i <- 1 to 7) assertEquals(buffer.onNext(i), Continue)
    s.tick()
    assertEquals(received, 28)

    for (i <- 0 to 2004) assertEquals(buffer.onNext(i), Continue)

    s.tick()
    assertEquals(received, 28)

    promise.success(Continue); s.tick()

    if (Platform.isJVM) {
      assertEquals(received, 28 + (2000 to 2004).sum)
      assertEquals(log.get(), 2000L)
    } else {
      assertEquals(log.get(), 2002L)
      assertEquals(received, 28 + (2002 to 2004).sum)
    }

    buffer.onComplete(); s.tick()
    assert(wasCompleted, "wasCompleted should be true")
  }

  fixture.test("should send onError when empty") { implicit s =>
    var errorThrown: Throwable = null
    val buffer = buildNewWithSignal(
      5,
      new Observer[Int] {
        def onError(ex: Throwable) = {
          errorThrown = ex
        }

        def onNext(elem: Int) = throw new IllegalStateException()
        def onComplete() = throw new IllegalStateException()
      }
    )

    buffer.onError(DummyException("dummy"))
    s.tickOne()

    assertEquals(errorThrown, DummyException("dummy"))
    val r = buffer.onNext(1)
    assertEquals(r, Stop)
  }

  fixture.test("should send onError when in flight") { implicit s =>
    var errorThrown: Throwable = null
    val buffer = buildNewWithSignal(
      5,
      new Observer[Int] {
        def onError(ex: Throwable) = {
          errorThrown = ex
        }
        def onNext(elem: Int) = Continue
        def onComplete() = throw new IllegalStateException()
      }
    )

    buffer.onNext(1)
    buffer.onError(DummyException("dummy"))
    s.tick()

    assertEquals(errorThrown, DummyException("dummy"))
  }

  fixture.test("should send onError when at capacity") { implicit s =>
    var errorThrown: Throwable = null
    val promise = Promise[Ack]()

    val buffer = buildNewWithSignal(
      5,
      new Observer[Int] {
        def onError(ex: Throwable) = {
          errorThrown = ex
        }
        def onNext(elem: Int) = promise.future
        def onComplete() = throw new IllegalStateException()
      }
    )

    for (i <- 1 to 10) assertEquals(buffer.onNext(i), Continue)
    buffer.onError(DummyException("dummy"))

    promise.success(Continue)
    s.tick()

    assertEquals(errorThrown, DummyException("dummy"))
  }

  fixture.test("should do onComplete only after all the queue was drained") { implicit s =>
    var sum = 0L
    var wasCompleted = false
    val startConsuming = Promise[Continue.type]()

    val buffer = buildNewWithSignal(
      10000,
      new Observer[Int] {
        def onNext(elem: Int) = {
          sum += elem
          startConsuming.future
        }
        def onError(ex: Throwable) = throw ex
        def onComplete() = wasCompleted = true
      }
    )

    (0 until 9999).foreach { x => buffer.onNext(x); () }
    buffer.onComplete()
    startConsuming.success(Continue)

    s.tick()
    assert(wasCompleted)
    assert(sum == (0 until 9999).sum)
  }

  fixture.test("should do onComplete only after all the queue was drained, test2") { implicit s =>
    var sum = 0L
    var wasCompleted = false

    val buffer = buildNewWithSignal(
      10000,
      new Observer[Int] {
        def onNext(elem: Int) = {
          sum += elem
          Continue
        }
        def onError(ex: Throwable) = throw ex
        def onComplete() = wasCompleted = true
      }
    )

    (0 until 9999).foreach { x => buffer.onNext(x); () }
    buffer.onComplete()
    s.tick()

    assert(wasCompleted)
    assert(sum == (0 until 9999).sum)
  }

  fixture.test("should do onError only after the queue was drained") { implicit s =>
    var sum = 0L
    var errorThrown: Throwable = null
    val startConsuming = Promise[Continue.type]()

    val buffer = buildNewWithSignal(
      10000,
      new Observer[Int] {
        def onNext(elem: Int) = {
          sum += elem
          startConsuming.future
        }
        def onError(ex: Throwable) = errorThrown = ex
        def onComplete() = throw new IllegalStateException()
      }
    )

    (0 until 9999).foreach { x => buffer.onNext(x); () }
    buffer.onError(DummyException("dummy"))
    startConsuming.success(Continue)

    s.tick()
    assertEquals(errorThrown, DummyException("dummy"))
    assertEquals(sum, (0 until 9999).sum.toLong)
  }

  fixture.test("should do onError only after all the queue was drained, test2") { implicit s =>
    var sum = 0L
    var errorThrown: Throwable = null

    val buffer = buildNewWithSignal(
      10000,
      new Observer[Int] {
        def onNext(elem: Int) = {
          sum += elem
          Continue
        }
        def onError(ex: Throwable) = errorThrown = ex
        def onComplete() = throw new IllegalStateException()
      }
    )

    (0 until 9999).foreach { x => buffer.onNext(x); () }
    buffer.onError(DummyException("dummy"))

    s.tick()
    assertEquals(errorThrown, DummyException("dummy"))
    assertEquals(sum, (0 until 9999).sum.toLong)
  }

  fixture.test("should do synchronous execution in batches") { implicit s =>
    var received = 0L
    var wasCompleted = false

    val buffer = buildNewWithSignal(
      Platform.recommendedBatchSize * 3,
      new Observer[Int] {
        def onNext(elem: Int) = {
          received += 1
          Continue
        }
        def onError(ex: Throwable) = ()
        def onComplete() = wasCompleted = true
      }
    )

    for (i <- 0 until (Platform.recommendedBatchSize * 2)) buffer.onNext(i)
    buffer.onComplete()
    assertEquals(received, 0L)

    s.tickOne()
    assertEquals(received, Platform.recommendedBatchSize.toLong)
    s.tickOne()
    assertEquals(received, Platform.recommendedBatchSize.toLong * 2)
    s.tickOne()
    assertEquals(wasCompleted, true)
  }
}
