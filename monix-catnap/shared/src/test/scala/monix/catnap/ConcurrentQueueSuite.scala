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

import java.util.concurrent.atomic.AtomicLong

import cats.effect.IO
import cats.implicits._
import minitest.TestSuite
import monix.execution.BufferCapacity.{ Bounded, Unbounded }
import monix.execution.ChannelType.{ MPMC, MPSC, SPMC, SPSC }
import monix.execution.{ BufferCapacity, ChannelType, Scheduler }
import monix.execution.internal.Platform
import monix.execution.schedulers.TestScheduler

import scala.collection.immutable.Queue
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import cats.effect.Temporal

object ConcurrentQueueFakeSuite extends BaseConcurrentQueueSuite[TestScheduler] {
  def setup() = TestScheduler()

  def tearDown(env: TestScheduler): Unit =
    assert(env.state.tasks.isEmpty, "should not have tasks left to execute")

  def testIO(name: String, times: Int)(f: Scheduler => IO[Unit]): Unit = {
    def repeatTest(test: IO[Unit], n: Int): IO[Unit] =
      if (n > 0) test.flatMap(_ => repeatTest(test, n - 1))
      else IO.unit

    test(name) { ec =>
      val result = repeatTest(f(ec), times).unsafeToFuture()
      ec.tick(1.day)
      result.value match {
        case None => throw new TimeoutException("1 day")
        case Some(value) => value.get
      }
    }
  }
}

object ConcurrentQueueGlobalSuite extends BaseConcurrentQueueSuite[Scheduler] {
  def setup() = Scheduler.global
  def tearDown(env: Scheduler): Unit = ()

  def testIO(name: String, times: Int)(f: Scheduler => IO[Unit]): Unit = {
    def repeatTest(test: IO[Unit], n: Int): IO[Unit] =
      if (n > 0) test.flatMap(_ => repeatTest(test, n - 1))
      else IO.unit

    testAsync(name) { implicit ec =>
      repeatTest(f(ec).timeout(60.seconds), times).unsafeToFuture()
    }
  }
}

abstract class BaseConcurrentQueueSuite[S <: Scheduler] extends TestSuite[S] {
  implicit def contextShift(implicit s: Scheduler): ContextShift[IO] =
    SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
  implicit def timer(implicit s: Scheduler): Temporal[IO] =
    SchedulerEffect.timerLiftIO[IO](s)(IO.ioEffect)

  val repeatForFastTests = {
    if (Platform.isJVM) 1000 else 100
  }
  val repeatForSlowTests = {
    if (Platform.isJVM) 50 else 1
  }

  /** TO IMPLEMENT ... */
  def testIO(name: String, times: Int = 1)(f: Scheduler => IO[Unit]): Unit

  testIO("simple offer and poll", times = repeatForFastTests) { implicit s =>
    for {
      queue <- ConcurrentQueue[IO].withConfig[Int](Bounded(10))
      _     <- queue.offer(1)
      _     <- queue.offer(2)
      _     <- queue.offer(3)
      r1    <- queue.poll
      r2    <- queue.poll
      r3    <- queue.poll
    } yield {
      assertEquals(r1, 1)
      assertEquals(r2, 2)
      assertEquals(r3, 3)
    }
  }

  testIO("async poll", times = repeatForFastTests) { implicit s =>
    for {
      queue <- ConcurrentQueue[IO].bounded[Int](10)
      _     <- queue.offer(1)
      r1    <- queue.poll
      _     <- IO(assertEquals(r1, 1))
      f     <- IO(queue.poll.unsafeToFuture())
      _     <- IO(assertEquals(f.value, None))
      _     <- queue.offer(2)
      r2    <- IO.fromFuture(IO.pure(f))
    } yield {
      assertEquals(r2, 2)
    }
  }

  testIO("offer/poll over capacity", times = repeatForFastTests) { implicit s =>
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(10))
    val count = 1000

    def producer(n: Int): IO[Unit] =
      if (n > 0) queue.offer(count - n).flatMap(_ => producer(n - 1))
      else IO.unit

    def consumer(n: Int, acc: Queue[Int] = Queue.empty): IO[Long] =
      if (n > 0)
        queue.poll.flatMap { a =>
          consumer(n - 1, acc.enqueue(a))
        }
      else
        IO.pure(acc.foldLeft(0L)(_ + _))

    for {
      p <- producer(count).start
      c <- consumer(count).start
      _ <- p.join
      r <- c.join
    } yield {
      assertEquals(r, count.toLong * (count - 1) / 2)
    }
  }

  testIO("tryOffer / tryPoll", times = repeatForFastTests) { implicit ec =>
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(16))
    val count = 1000

    def producer(n: Int): IO[Unit] =
      if (n > 0) queue.tryOffer(count - n).flatMap {
        case true =>
          producer(n - 1)
        case false =>
          IO.shift *> producer(n)
      }
      else {
        IO.unit
      }

    def consumer(n: Int, acc: Queue[Int] = Queue.empty): IO[Long] =
      if (n > 0)
        queue.tryPoll.flatMap {
          case Some(a) => consumer(n - 1, acc.enqueue(a))
          case None => IO.shift *> consumer(n, acc)
        }
      else
        IO.pure(acc.foldLeft(0L)(_ + _))

    for {
      p <- producer(count).start
      c <- consumer(count).start
      _ <- p.join
      r <- c.join
    } yield {
      assertEquals(r, count.toLong * (count - 1) / 2)
    }
  }

  testIO("drain; MPMC; unbounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Unbounded(), MPMC)
  }

  testIO("drain; MPSC; unbounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Unbounded(), MPSC)
  }

  testIO("drain; SPMC; unbounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Unbounded(), SPMC)
  }

  testIO("drain; SPMC; unbounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Unbounded(), SPSC)
  }

  testIO("drain; MPMC; bounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Bounded(32), MPMC)
  }

  testIO("drain; MPSC; bounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Bounded(32), MPSC)
  }

  testIO("drain; SPMC; bounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Bounded(32), SPMC)
  }

  testIO("drain; SPMC; bounded", times = repeatForFastTests) { implicit ec =>
    testDrain(Bounded(32), SPSC)
  }

  def testDrain(bc: BufferCapacity, ct: ChannelType)(implicit ec: Scheduler): IO[Unit] = {
    val count = 1000
    val elems = for (i <- 0 until count) yield i

    for {
      queue <- ConcurrentQueue[IO].withConfig[Int](bc, ct)
      f1    <- queue.drain(1000, 1000).start
      f2    <- queue.offerMany(elems).start
      _     <- f2.join
      r     <- f1.join
    } yield {
      assertEquals(r.sum, count * (count - 1) / 2)
    }
  }

  testIO("clear") { implicit s =>
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(10))

    queue.offer(1).unsafeRunSync()
    queue.clear.unsafeRunSync()

    val value = queue.tryPoll.unsafeRunSync()
    assertEquals(value, None)

    for {
      queue <- ConcurrentQueue[IO].bounded[Int](10)
      _     <- queue.offer(1)
      _     <- queue.clear
      r     <- queue.tryPoll
    } yield {
      assertEquals(r, None)
    }
  }

  testIO("clear after overflow") { implicit ec =>
    def fillQueue(queue: ConcurrentQueue[IO, Int]): IO[Unit] =
      queue.tryOffer(1).flatMap {
        case true => fillQueue(queue)
        case false => IO.unit
      }

    def consume(queue: ConcurrentQueue[IO, Int], acc: Int = 0): IO[Int] =
      queue.poll.flatMap { n =>
        if (n == 0) IO.pure(acc)
        else consume(queue, acc + n)
      }

    for {
      queue <- ConcurrentQueue[IO].bounded[Int](512)
      _     <- fillQueue(queue)
      _     <- queue.offerMany((0 until 500).map(_ => 1)).start
      _     <- queue.clear
      _     <- queue.offer(0)
      r     <- consume(queue)
    } yield {
      assert(r <= 500)
    }
  }

  testIO("queue should be empty") { implicit s =>
    for {
      queue <- ConcurrentQueue[IO].bounded[Int](10)
      _     <- queue.offer(1)
      _     <- queue.clear
      value <- queue.tryPoll
    } yield {
      assertEquals(value, None)
      assertEquals(queue.isEmpty.unsafeRunSync(), true)
    }
  }

  testIO("concurrent producer - consumer; MPMC; bounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(128), MPMC)
    testConcurrency(queue, count, 3)
  }

  testIO("concurrent producer - consumer; MPMC; unbounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Unbounded(), MPMC)
    testConcurrency(queue, count, 3)
  }

  testIO("concurrent producer - consumer; MPSC; bounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(128), MPSC)
    testConcurrency(queue, count, 1)
  }

  testIO("concurrent producer - consumer; MPSC; unbounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Unbounded(), MPSC)
    testConcurrency(queue, count, 1)
  }

  testIO("concurrent producer - consumer; SPMC; bounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(128), SPMC)
    testConcurrency(queue, count, 3)
  }

  testIO("concurrent producer - consumer; SPMC; unbounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Unbounded(), SPMC)
    testConcurrency(queue, count, 3)
  }

  testIO("concurrent producer - consumer; SPSC; bounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Bounded(128), SPSC)
    testConcurrency(queue, count, 1)
  }

  testIO("concurrent producer - consumer; SPSC; unbounded") { implicit ec =>
    val count = if (Platform.isJVM) 10000 else 1000
    val queue = ConcurrentQueue[IO].unsafe[Int](Unbounded(), SPSC)
    testConcurrency(queue, count, 1)
  }

  def testConcurrency(queue: ConcurrentQueue[IO, Int], n: Int, workers: Int)(implicit s: Scheduler): IO[Unit] = {

    def producer(n: Int): IO[Unit] = {
      def offerViaTry(n: Int): IO[Unit] =
        queue.tryOffer(n).flatMap {
          case true => IO.unit
          case false => offerViaTry(n)
        }

      if (n > 0) {
        val offer = if (n % 2 == 0) queue.offer(n) else offerViaTry(n)
        offer.flatMap(_ => producer(n - 1))
      } else {
        queue.offerMany(for (_ <- 0 until workers) yield 0)
      }
    }

    val atomic = new AtomicLong(0)
    def consumer(idx: Int = 0): IO[Unit] = {
      def pollViaTry: IO[Int] =
        queue.tryPoll.flatMap {
          case Some(v) => IO.pure(v)
          case None => IO.shift *> pollViaTry
        }

      val poll = if (idx % 2 == 0) queue.poll else pollViaTry
      poll.flatMap { i =>
        if (i > 0) {
          atomic.addAndGet(i.toLong)
          consumer(idx + 1)
        } else {
          IO.unit
        }
      }
    }

    val tasks = (producer(n) +: (0 until workers).map(_ => consumer())).toList
    for (_ <- tasks.parSequence) yield {
      assertEquals(atomic.get(), n.toLong * (n + 1) / 2)
    }
  }
}
