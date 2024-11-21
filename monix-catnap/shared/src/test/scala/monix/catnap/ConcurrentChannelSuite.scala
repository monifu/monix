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

import cats.effect.IO
import cats.implicits._
import minitest.TestSuite
import monix.execution.BufferCapacity.{ Bounded, Unbounded }
import monix.execution.ChannelType.{ MPMC, MPSC, SPMC, SPSC }
import monix.execution.exceptions.APIContractViolationException
import monix.execution.internal.Platform
import monix.execution.schedulers.TestScheduler
import monix.execution.{ BufferCapacity, Scheduler, TestUtils }

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import cats.effect.Temporal

object ConcurrentChannelFakeSuite extends BaseConcurrentChannelSuite[TestScheduler] {
  def setup() = TestScheduler()
  def tearDown(env: TestScheduler): Unit =
    assert(env.state.tasks.isEmpty, "There should be no tasks left!")

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

  val boundedConfigForConcurrentSum: Bounded =
    Bounded(256)
}

abstract class BaseConcurrentChannelSuite[S <: Scheduler] extends TestSuite[S] with TestUtils {
  val boundedConfigForConcurrentSum: Bounded

  val iterationsCount = {
    if (isCI) 50
    else if (Platform.isJVM) 10000
    else 100
  }

  val repeatForFastTests = {
    if (Platform.isJVM) 1000 else 100
  }

  val repeatForSlowTests = {
    if (Platform.isJVM) 50 else 1
  }

  val boundedConfig = ConsumerF.Config(capacity = Some(Bounded(10)))
  val unboundedConfig = ConsumerF.Config(capacity = Some(Unbounded()))

  implicit def contextShift(implicit s: Scheduler): ContextShift[IO] =
    SchedulerEffect.contextShift[IO](s)(IO.ioEffect)
  implicit def timer(implicit s: Scheduler): Temporal[IO] =
    SchedulerEffect.timerLiftIO[IO](s)(IO.ioEffect)

  /** TO IMPLEMENT ... */
  def testIO(name: String, times: Int = 1)(f: Scheduler => IO[Unit]): Unit

  testIO("simple push and pull", times = repeatForFastTests) { implicit ec =>
    for {
      chan <- ConcurrentChannel[IO].withConfig[Int, Int](boundedConfig)
      consume = chan.consume.use { consumer =>
        for {
          r1 <- consumer.pull
          r2 <- consumer.pull
          r3 <- consumer.pull
          r4 <- consumer.pull
        } yield {
          assertEquals(r1, Right(1))
          assertEquals(r2, Right(2))
          assertEquals(r3, Right(3))
          assertEquals(r4, Left(0))
        }
      }
      fiber <- consume.start
      _     <- chan.awaitConsumers(1)
      _     <- chan.push(1)
      _     <- chan.push(2)
      _     <- chan.push(3)
      _     <- chan.halt(0)
      r     <- fiber.join
    } yield r
  }

  testIO("consumers can receive push", times = repeatForFastTests) { implicit ec =>
    for {
      chan  <- ConcurrentChannel[IO].withConfig[Int, Int](boundedConfig)
      fiber <- chan.consume.use(_.pull).start
      _     <- chan.awaitConsumers(1)
      _     <- chan.push(1)
      r     <- fiber.join
    } yield {
      assertEquals(r, Right(1))
    }
  }

  testIO("consumers can wait for push", times = repeatForSlowTests) { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int], acc: Int = 0): IO[Int] =
      c.pull.flatMap {
        case Left(l) => IO.pure(acc + l)
        case Right(r) => consume(c, acc + r)
      }

    for {
      chan  <- ConcurrentChannel[IO].withConfig[Int, Int](boundedConfig)
      fiber <- chan.consume.use(consume(_)).start
      _     <- chan.awaitConsumers(1)
      _     <- IO.sleep(3.millis)
      _     <- chan.push(1)
      _     <- IO.shift *> IO.shift *> chan.push(2)
      _     <- IO.sleep(3.millis)
      _     <- chan.push(3)
      _     <- chan.halt(4)
      r     <- fiber.join
    } yield {
      assertEquals(r, 1 + 2 + 3 + 4)
    }
  }

  testIO("consumers can receive pushMany", times = repeatForFastTests) { implicit ec =>
    for {
      chan  <- ConcurrentChannel[IO].withConfig[Int, Int](boundedConfig)
      fiber <- chan.consume.use(_.pullMany(10, 10)).start
      _     <- chan.awaitConsumers(1)
      _     <- chan.pushMany(1 to 10)
      r     <- fiber.join.map(_.map(_.sum))
    } yield {
      assertEquals(r, Right(55))
    }
  }

  testIO("consumers can wait for pushMany", times = repeatForSlowTests) { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int], acc: Int = 0): IO[Int] =
      c.pull.flatMap {
        case Left(l) => IO.pure(acc + l)
        case Right(r) => consume(c, acc + r)
      }

    for {
      chan  <- ConcurrentChannel[IO].withConfig[Int, Int](boundedConfig)
      fiber <- chan.consume.use(consume(_)).start
      _     <- chan.awaitConsumers(1)
      _     <- IO.sleep(3.millis)
      _     <- chan.pushMany(1 to 20)
      _     <- IO.shift *> IO.shift *> chan.pushMany(21 to 40)
      _     <- IO.sleep(3.millis)
      _     <- chan.pushMany(41 to 60)
      _     <- chan.halt(100)
      r     <- fiber.join
    } yield {
      assertEquals(r, 100 + 30 * 61)
    }
  }

  testIO("pullMany back-pressuring for minLength, with maxLength", times = repeatForFastTests) { implicit ec =>
    val channel = ConcurrentChannel[IO].unsafe[Int, Int]()
    val batch = channel.consume
      .use(_.pullMany(10, 10))
      .map {
        case l @ Left(_) => l
        case Right(seq) =>
          assertEquals(seq.length, 10)
          Right(seq.sum)
      }
      .start

    def loop(n: Int): IO[Unit] =
      channel.push(n).flatMap { _ =>
        if (n - 1 > 0) loop(n - 1)
        else IO.unit
      }

    for {
      f <- batch
      _ <- channel.awaitConsumers(1)
      _ <- loop(9)
      _ <- loop(10)
      r <- f.join
    } yield {
      assertEquals(r, Right(5 * 11))
    }
  }

  testIO("subscribe after channel was closed") { implicit ec =>
    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      _       <- channel.push(1)
      _       <- channel.halt(0)
      r       <- channel.consume.use(_.pull)
    } yield {
      assertEquals(r, Left(0))
    }
  }

  testIO("push after channel was closed") { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int]): IO[Unit] =
      c.pull.flatMap {
        case Right(_) =>
          IO.raiseError(new APIContractViolationException("push after halt"))
        case Left(value) =>
          assertEquals(value, 0)
          IO.sleep(1.milli) *> consume(c)
      }

    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      _       <- channel.halt(0)
      fiber   <- channel.consume.use(consume).start
      b1      <- channel.push(1)
      b2      <- channel.push(2)
      _       <- channel.halt(10)
      _       <- fiber.join.timeoutTo(10.millis, IO.unit).guarantee(fiber.cancel)
    } yield {
      assertEquals(b1, false)
      assertEquals(b2, false)
    }
  }

  testIO("pushMany after channel was closed") { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int]): IO[Unit] =
      c.pull.flatMap {
        case Right(_) =>
          IO.raiseError(new APIContractViolationException("push after halt"))
        case Left(value) =>
          assertEquals(value, 0)
          IO.sleep(1.milli) *> consume(c)
      }

    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      _       <- channel.halt(0)
      fiber   <- channel.consume.use(consume).start
      b1      <- channel.pushMany(Seq(1, 2, 3))
      _       <- channel.halt(10)
      _       <- fiber.join.timeoutTo(10.millis, IO.unit).guarantee(fiber.cancel)
    } yield {
      assertEquals(b1, false)
    }
  }

  testIO("push/pushMany with no consumers") { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int], acc: Int = 0): IO[Int] =
      c.pull.flatMap {
        case Left(l) => IO.pure(acc + l)
        case Right(r) => consume(c, acc + r)
      }

    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      _       <- channel.push(1)
      _       <- channel.push(2)
      _       <- channel.push(3)
      _       <- channel.pushMany(Seq(4, 5, 6))
      fiber   <- channel.consume.use(consume(_)).start
      _       <- channel.awaitConsumers(1)
      _       <- channel.push(100)
      _       <- channel.pushMany(Seq(100, 100))
      _       <- channel.halt(100)
      r       <- fiber.join
    } yield {
      assertEquals(r, 400)
    }
  }

  testIO("pushMany with multiple consumers") { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int], acc: Int = 0): IO[Int] =
      c.pull.flatMap {
        case Left(l) => IO.pure(acc + l)
        case Right(r) => consume(c, acc + r)
      }

    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      fiber1  <- channel.consume.use(consume(_)).start
      fiber2  <- channel.consume.use(consume(_)).start
      fiber3  <- channel.consume.use(consume(_)).start
      _       <- channel.awaitConsumers(3)
      _       <- channel.push(100)
      _       <- channel.pushMany(Seq(100, 100))
      _       <- channel.halt(100)
      r1      <- fiber1.join
      r2      <- fiber2.join
      r3      <- fiber3.join
    } yield {
      assertEquals(r1, 400)
      assertEquals(r2, 400)
      assertEquals(r3, 400)
    }
  }

  testIO("halt with awaitConsumers active") { implicit ec =>
    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      await   <- channel.awaitConsumers(3).start
      _       <- await.join.timeoutTo(1.millis, IO.unit)
      _       <- channel.halt(0)
      r       <- await.join
    } yield {
      assertEquals(r, false)
    }
  }

  testIO("awaitConsumers after halt") { implicit ec =>
    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      _       <- channel.halt(0)
      r       <- channel.awaitConsumers(3)
    } yield {
      assertEquals(r, false)
    }
  }

  testIO("awaitConsumers after consume, consume/release, consume, consume") { implicit ec =>
    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      c1      <- channel.consume.use(c => c.pull *> c.pull).start
      await   <- channel.awaitConsumers(3).start
      c2      <- channel.consume.use(c => c.pull).start
      _       <- await.join.timeoutTo(3.millis, IO.unit)
      _       <- channel.push(1)
      r2      <- c2.join
      c3      <- channel.consume.use(c => c.pull).start
      c4      <- channel.consume.use(c => c.pull).start
      _       <- await.join
      _       <- channel.halt(0)
      r1      <- c1.join
      r3      <- c3.join
      r4      <- c4.join
    } yield {
      assertEquals(r1, Left(0))
      assertEquals(r2, Right(1))
      assertEquals(r3, Left(0))
      assertEquals(r4, Left(0))
    }
  }

  testIO("pushMany with empty sequence") { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int], acc: Int = 0): IO[Int] =
      c.pull.flatMap {
        case Left(l) => IO.pure(acc + l)
        case Right(r) => consume(c, acc + r)
      }

    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      fiber   <- channel.consume.use(consume(_)).start
      _       <- channel.awaitConsumers(1)
      _       <- channel.pushMany(Seq.empty)
      _       <- channel.halt(100)
      r       <- fiber.join
    } yield {
      assertEquals(r, 100)
    }
  }

  testIO("cancellation of paused pull") { implicit ec =>
    def consume(c: ConsumerF[IO, Int, Int], acc: Int = 0): IO[Int] =
      c.pull.flatMap {
        case Left(l) => IO.pure(acc + l)
        case Right(r) => consume(c, acc + r)
      }

    for {
      channel <- ConcurrentChannel[IO].of[Int, Int]
      fiber   <- channel.consume.use(consume(_)).start
      _       <- channel.awaitConsumers(1)
      _       <- fiber.cancel
    } yield ()
  }

  testIO(
    s"concurrent sum via consumer.pull; MPMC; producers=4, consumers=4, workers=4, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 4,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO("concurrent sum via consumer.pull; MPMC; producers=4, consumers=4, workers=4, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 4,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO(
    s"concurrent sum via consumer.pull; SPMC; producers=1, consumers=4, workers=4, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 4,
        workersPerConsumer = 4,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO("concurrent sum via consumer.pull; SPMC; producers=1, consumers=4, workers=4, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 4,
        workersPerConsumer = 4,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO(
    s"concurrent sum via consumer.pull; MPMC; producers=4, consumers=1, workers=4, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 1,
        workersPerConsumer = 4,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO("concurrent sum via consumer.pull; MPMC; producers=4, consumers=1, workers=4, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 1,
        workersPerConsumer = 4,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO(
    s"concurrent sum via consumer.pull; MPSC; producers=4, consumers=4, workers=1, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 1,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO("concurrent sum via consumer.pull; MPSC; producers=4, consumers=4, workers=1, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 1,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO(
    s"concurrent sum via consumer.pull; SPSC; producers=1, consumers=1, workers=1, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 1,
        workersPerConsumer = 1,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO("concurrent sum via consumer.pull; SPSC; producers=1, consumers=1, workers=1, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 1,
        workersPerConsumer = 1,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = false
      )
  }

  testIO(
    s"concurrent sum via consumer.pullMany; MPMC; producers=4, consumers=4, workers=4, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 4,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO("concurrent sum via consumer.pullMany; MPMC; producers=4, consumers=4, workers=4, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 4,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO(
    s"concurrent sum via consumer.pullMany; SPMC; producers=1, consumers=4, workers=4, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 4,
        workersPerConsumer = 4,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO("concurrent sum via consumer.pullMany; SPMC; producers=1, consumers=4, workers=4, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 4,
        workersPerConsumer = 4,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO(
    s"concurrent sum via consumer.pullMany; MPMC; producers=4, consumers=1, workers=4, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 1,
        workersPerConsumer = 4,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO("concurrent sum via consumer.pullMany; MPMC; producers=4, consumers=1, workers=4, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 1,
        workersPerConsumer = 4,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO(
    s"concurrent sum via consumer.pullMany; MPSC; producers=4, consumers=4, workers=1, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 1,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO("concurrent sum via consumer.pullMany; MPSC; producers=4, consumers=4, workers=1, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 4,
        consumers = 4,
        workersPerConsumer = 1,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO(
    s"concurrent sum via consumer.pullMany; SPSC; producers=1, consumers=1, workers=1, capacity=$boundedConfigForConcurrentSum"
  ) {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 1,
        workersPerConsumer = 1,
        boundedConfigForConcurrentSum,
        count = iterationsCount,
        pullMany = true
      )
  }

  testIO("concurrent sum via consumer.pullMany; SPSC; producers=1, consumers=1, workers=1, capacity=Unbounded") {
    implicit ec =>
      testConcurrentSum(
        producers = 1,
        consumers = 1,
        workersPerConsumer = 1,
        capacity = Unbounded(),
        count = iterationsCount,
        pullMany = true
      )
  }

  def testConcurrentSum(
    producers: Int,
    consumers: Int,
    workersPerConsumer: Int,
    capacity: BufferCapacity,
    count: Int,
    pullMany: Boolean
  )(implicit ec: Scheduler): IO[Unit] = {

    val channelType =
      if (producers > 1) {
        if (workersPerConsumer > 1) MPMC
        else MPSC
      } else {
        if (workersPerConsumer > 1) SPMC
        else SPSC
      }

    def consume(consumer: ConsumerF[IO, Int, Int]): IO[Long] = {
      def worker(acc: Long): IO[Long] = {
        if (pullMany)
          consumer.pullMany(1, 16).flatMap {
            case Left(i) => IO.pure(acc + i)
            case Right(seq) =>
              assert(seq.length <= 16, s"seq.length (${seq.length}) <= 16")
              worker(acc + seq.sum)
          }
        else
          consumer.pull.flatMap {
            case Left(i) => IO.pure(acc + i)
            case Right(i) => worker(acc + i)
          }
      }

      if (workersPerConsumer > 1) {
        val list = (0 until workersPerConsumer).map(_ => worker(0)).toList
        list.parSequence.map(_.sum)
      } else {
        worker(0)
      }
    }

    def consumeMany(channel: ConcurrentChannel[IO, Int, Int]): IO[Long] = {
      val task = channel
        .consumeWithConfig(ConsumerF.Config(Some(capacity), Some(channelType.consumerType), None))
        .use(ref => consume(ref))

      if (consumers < 2) {
        task
      } else {
        val list = (0 until consumers).map(_ => task).toList
        list.parSequence.map(_.sum)
      }
    }

    def produce(channel: ConcurrentChannel[IO, Int, Int]): IO[Unit] = {
      def loop(channel: ConcurrentChannel[IO, Int, Int], n: Int): IO[Unit] =
        if (n > 0) channel.push(n).flatMap(_ => loop(channel, n - 1))
        else IO.unit

      val task = loop(channel, count)
      if (producers < 2)
        task
      else
        (0 until producers).map(_ => task).toList.parSequence_
    }

    for {
      channel <- ConcurrentChannel[IO].withConfig[Int, Int](producerType = channelType.producerType)
      fiber   <- consumeMany(channel).start
      _       <- channel.awaitConsumers(consumers)
      _       <- produce(channel)
      _       <- channel.halt(0)
      sum     <- fiber.join
    } yield {
      val perProducer = count.toLong * (count + 1) / 2
      assertEquals(sum, perProducer * producers * consumers)
    }
  }
}
