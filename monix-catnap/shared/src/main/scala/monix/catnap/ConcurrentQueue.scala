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

import cats.effect.Concurrent
import cats.implicits._
import monix.catnap.internal.QueueHelpers
import monix.execution.BufferCapacity.{ Bounded, Unbounded }
import monix.execution.ChannelType.MPMC
import monix.execution.annotations.{ UnsafeBecauseImpure, UnsafeProtocol }
import monix.execution.atomic.AtomicAny
import monix.execution.atomic.PaddingStrategy.LeftRight128
import monix.execution.internal.Constants
import monix.execution.internal.collection.{ LowLevelConcurrentQueue => LowLevelQueue }
import monix.execution.{ BufferCapacity, CancelablePromise, ChannelType }
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
  * A high-performance, back-pressured, generic concurrent queue implementation.
  *
  * This is the pure and generic version of [[monix.execution.AsyncQueue]].
  *
  * ==Example==
  *
  * {{{
  *   import cats.implicits._
  *   import cats.effect._
  *   import monix.execution.Scheduler.global
  *
  *   // For being able to do IO.start
  *   implicit val cs: ContextShift[IO] = SchedulerEffect.contextShift[IO](global)(IO.ioEffect)
  *   // We need a `Timer` for this to work
  *   implicit val timer: Timer[IO] = SchedulerEffect.timer[IO](global)
  *
  *   def consumer(queue: ConcurrentQueue[IO, Int], index: Int): IO[Unit] =
  *     queue.poll.flatMap { a =>
  *       println(s"Worker $$index: $$a")
  *       consumer(queue, index)
  *     }
  *
  *   for {
  *     queue     <- ConcurrentQueue[IO].bounded[Int](capacity = 32)
  *     consumer1 <- consumer(queue, 1).start
  *     consumer2 <- consumer(queue, 1).start
  *     // Pushing some samples
  *     _         <- queue.offer(1)
  *     _         <- queue.offer(2)
  *     _         <- queue.offer(3)
  *     // Stopping the consumer loops
  *     _         <- consumer1.cancel
  *     _         <- consumer2.cancel
  *   } yield ()
  * }}}
  *
  * ==Back-Pressuring and the Polling Model==
  *
  * The initialized queue can be limited to a maximum buffer size, a size
  * that could be rounded to a power of 2, so you can't rely on it to be
  * precise. Such a bounded queue can be initialized via
  * [[monix.catnap.ConcurrentQueue.bounded ConcurrentQueue.bounded]].
  * Also see [[monix.execution.BufferCapacity BufferCapacity]], the
  * configuration parameter that can be passed in the
  * [[monix.catnap.ConcurrentQueue.withConfig ConcurrentQueue.withConfig]]
  * builder.
  *
  * On [[offer]], when the queue is full, the implementation back-pressures
  * until the queue has room again in its internal buffer, the task being
  * completed when the value was pushed successfully. Similarly [[poll]] awaits
  * the queue to have items in it. This works for both bounded and unbounded queues.
  *
  * For both `offer` and `poll`, in case awaiting a result happens, the
  * implementation does so asynchronously, without any threads being blocked.
  *
  * ==Multi-threading Scenario==
  *
  * This queue supports a [[monix.execution.ChannelType ChannelType]]
  * configuration, for fine tuning depending on the needed multi-threading
  * scenario. And this can yield better performance:
  *
  *   - [[monix.execution.ChannelType.MPMC MPMC]]:
  *     multi-producer, multi-consumer
  *   - [[monix.execution.ChannelType.MPSC MPSC]]:
  *     multi-producer, single-consumer
  *   - [[monix.execution.ChannelType.SPMC SPMC]]:
  *     single-producer, multi-consumer
  *   - [[monix.execution.ChannelType.SPSC SPSC]]:
  *     single-producer, single-consumer
  *
  * The default is `MPMC`, because that's the safest scenario.
  *
  * {{{
  *   import monix.execution.ChannelType.MPSC
  *   import monix.execution.BufferCapacity.Bounded
  *
  *   val queue = ConcurrentQueue[IO].withConfig[Int](
  *     capacity = Bounded(128),
  *     channelType = MPSC
  *   )
  * }}}
  *
  * '''WARNING''': default is `MPMC`, however any other scenario implies
  * a relaxation of the internal synchronization between threads.
  *
  * This means that using the wrong scenario can lead to severe
  * concurrency bugs. If you're not sure what multi-threading scenario you
  * have, then just stick with the default `MPMC`.
  */
final class ConcurrentQueue[F[_], A] private (
  capacity: BufferCapacity,
  channelType: ChannelType
)(implicit F: Concurrent[F], cs: ContextShift[F])
  extends Serializable {

  /** Try pushing a value to the queue.
    *
    * The protocol is unsafe because usage of the "try*" methods imply an
    * understanding of concurrency, or otherwise the code can be very
    * fragile and buggy.
    *
    * @param a is the value pushed in the queue
    *
    * @return `true` if the operation succeeded, or `false` if the queue is
    *         full and cannot accept any more elements
    */
  @UnsafeProtocol
  def tryOffer(a: A): F[Boolean] = F.delay(tryOfferUnsafe(a))

  /** Pushes a value in the queue, or if the queue is full, then repeats the
    * operation until it succeeds.
    *
    * @return a task that when evaluated, will complete with a value,
    *         or wait until such a value is ready
    */
  def offer(a: A): F[Unit] = F.defer {
    if (tryOfferUnsafe(a))
      F.unit
    else
      offerWait(a)
  }

  /** Pushes multiple values in the queue. Back-pressures if the queue is full.
    *
    * @return a task that will eventually complete when the
    *         push has succeeded; it can also be cancelled, interrupting the
    *         waiting
    */
  def offerMany(seq: Iterable[A]): F[Unit] = {
    // Recursive, async loop
    def loop(cursor: Iterator[A]): F[Unit] = {
      var elem: A = null.asInstanceOf[A]
      var hasCapacity = true
      // Happy path
      while (hasCapacity && cursor.hasNext) {
        elem = cursor.next()
        hasCapacity = queue.offer(elem) == 0
      }
      // Awaken sleeping consumers
      notifyConsumers()
      // Do we need to await on consumers?
      if (!hasCapacity) {
        offerWait(elem).flatMap(_ => loop(cursor))
      } else {
        F.unit
      }
    }

    F.defer {
      val cursor = seq.iterator
      loop(cursor)
    }
  }

  /** Try pulling a value out of the queue.
    *
    * The protocol is unsafe because usage of the "try*" methods imply an
    * understanding of concurrency, or otherwise the code can be very
    * fragile and buggy.
    *
    * @return `Some(a)` in case a value was successfully retrieved from the
    *         queue, or `None` in case the queue is empty
    */
  @UnsafeProtocol
  def tryPoll: F[Option[A]] = tryPollRef
  private[this] val tryPollRef =
    F.delay(Option(tryPollUnsafe()))

  /** Fetches a value from the queue, or if the queue is empty it awaits
    * asynchronously until a value is made available.
    *
    * @return a task that when evaluated, will eventually complete
    *         after the value has been successfully pushed in the queue
    */
  def poll: F[A] = pollRef
  private[this] val pollRef = F.defer[A] {
    val happy = tryPollUnsafe()
    // noinspection ForwardReference
    if (happy != null)
      F.pure(happy)
    else
      F.asyncF { cb =>
        helpers.sleepThenRepeat(
          consumersAwaiting,
          pollQueue,
          pollTest,
          pollMap,
          cb
        )
      }
  }

  /** Fetches multiple elements from the queue, if available.
    *
    * This operation back-pressures until the `minLength` requirement is
    * achieved.
    *
    * @param minLength specifies the minimum length of the returned sequence;
    *        the operation back-pressures until this length is satisfied
    *
    * @param maxLength is the capacity of the used buffer, being the max
    *        length of the returned sequence
    *
    * @return a future with a sequence of length between minLength and maxLength;
    *         it can also be cancelled, interrupting the wait
    */
  def drain(minLength: Int, maxLength: Int): F[Seq[A]] =
    F.defer {
      assert(minLength <= maxLength, s"minLength ($minLength) <= maxLength ($maxLength")
      val buffer = ArrayBuffer.empty[A]
      val length = tryDrainUnsafe(buffer, maxLength)

      // Happy path
      if (length >= minLength) {
        F.pure(toSeq(buffer))
      } else {
        // Going async
        F.asyncF { cb =>
          helpers.sleepThenRepeat[Int, Seq[A]](
            consumersAwaiting,
            () => tryDrainUnsafe(buffer, maxLength - buffer.length),
            _ => buffer.length >= minLength,
            _ => toSeq(buffer),
            cb
          )
        }
      }
    }

  /** Removes all items from the queue.
    *
    * Called from the consumer thread, subject to the restrictions appropriate
    * to the implementation indicated by
    * [[monix.execution.ChannelType ChannelType]].
    *
    * '''WARNING:''' the `clear` operation should be done on the consumer side,
    * so it must be called from the same thread(s) that call [[poll]].
    */
  def clear: F[Unit] = clearRef
  // noinspection ForwardReference
  private[this] val clearRef = F.delay {
    queue.clear()
    notifyProducers()
  }

  /** Checks if the queue is empty.
    *
    * '''UNSAFE PROTOCOL:'''
    * Concurrent shared state changes very frequently, therefore this function might yield nondeterministic results.
    * Should be used carefully since some usecases might require a deeper insight into concurrent programming.
    */
  @UnsafeProtocol
  def isEmpty: F[Boolean] =
    F.delay(queue.isEmpty)

  private def tryOfferUnsafe(a: A): Boolean = {
    if (queue.offer(a) == 0) {
      notifyConsumers()
      true
    } else {
      false
    }
  }

  private def tryPollUnsafe(): A = {
    val a = queue.poll()
    notifyProducers()
    a
  }

  private def tryDrainUnsafe(buffer: ArrayBuffer[A], maxLength: Int): Int = {
    val length = queue.drainToBuffer(buffer, maxLength)
    if (length > 0) notifyProducers()
    length
  }

  @tailrec
  private def notifyConsumers(): Unit = {
    // N.B. in case the queue is single-producer, this is a full memory fence
    // meant to prevent the re-ordering of `queue.offer` with `consumersAwait.get`
    queue.fenceOffer()

    val ref = consumersAwaiting.get()
    if (ref ne null) {
      if (consumersAwaiting.compareAndSet(ref, null)) {
        ref.complete(Constants.successOfUnit)
        ()
      } else {
        notifyConsumers()
      }
    }
  }

  @tailrec
  private def notifyProducers(): Unit =
    if (producersAwaiting ne null) {
      // N.B. in case this isn't a multi-consumer queue, this generates a
      // full memory fence in order to prevent the re-ordering of queue.poll()
      // with `producersAwait.get`
      queue.fencePoll()

      val ref = producersAwaiting.get()
      if (ref ne null) {
        if (producersAwaiting.compareAndSet(ref, null)) {
          ref.complete(Constants.successOfUnit)
          ()
        } else {
          notifyProducers()
        }
      }
    }

  private def offerWait(a: A): F[Unit] =
    F.asyncF { cb =>
      helpers.sleepThenRepeat(
        producersAwaiting,
        () => tryOfferUnsafe(a),
        offerTest,
        offerMap,
        cb
      )
    }

  private[this] val queue: LowLevelQueue[A] =
    LowLevelQueue(capacity, channelType, fenced = true)
  private[this] val helpers: QueueHelpers[F] =
    new QueueHelpers[F]

  private[this] val consumersAwaiting =
    AtomicAny.withPadding[CancelablePromise[Unit]](null, LeftRight128)

  private[this] val producersAwaiting =
    if (capacity.isBounded)
      AtomicAny.withPadding[CancelablePromise[Unit]](null, LeftRight128)
    else
      null

  private[this] val pollQueue: () => A = () => tryPollUnsafe()
  private[this] val pollTest: A => Boolean = _ != null
  private[this] val pollMap: A => A = a => a
  private[this] val offerTest: Boolean => Boolean = x => x
  private[this] val offerMap: Boolean => Unit = _ => ()

  private def toSeq(buffer: ArrayBuffer[A]): Seq[A] =
    buffer.toArray[Any].toSeq.asInstanceOf[Seq[A]]
}

/**
  * @define channelTypeDesc (UNSAFE) specifies the concurrency scenario, for
  *         fine tuning the performance
  *
  * @define bufferCapacityParam specifies the `BufferCapacity`, which can be
  *         either "bounded" (with a maximum capacity), or "unbounded"
  *
  * @define concurrentParam is a `cats.effect.Concurrent` type class restriction;
  *         this queue is built to work with `Concurrent` data types
  *
  * @define csParam is a `ContextShift`, needed for triggering async boundaries
  *         for fairness reasons, in case there's a need to back-pressure on
  *         the internal buffer
  */
object ConcurrentQueue {
  /**
    * Builds an [[ConcurrentQueue]] value for `F` data types that implement
    * the `Concurrent` type class.
    *
    * This builder uses the
    * [[https://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially-Applied Type]]
    * technique.
    *
    * @param F $concurrentParam
    */
  def apply[F[_]](implicit F: Concurrent[F]): ApplyBuilders[F] =
    new ApplyBuilders[F](F)

  /**
    * Builds a limited capacity and back-pressured [[ConcurrentQueue]].
    *
    * @see [[unbounded]] for building an unbounded queue that can use the
    *      entire memory available to the process.
    *
    * @param capacity is the maximum capacity of the internal buffer; note
    *        that due to performance optimizations, the capacity of the internal
    *        buffer can get rounded to a power of 2, so the actual capacity may
    *        be slightly different than the one specified
    *
    * @param cs $csParam
    * @param F $concurrentParam
    */
  def bounded[F[_], A](capacity: Int)(implicit F: Concurrent[F]): F[ConcurrentQueue[F, A]] =
    withConfig(Bounded(capacity), MPMC)

  /**
    * Builds an unlimited [[ConcurrentQueue]] that can use the entire memory
    * available to the process.
    *
    * @see [[bounded]] for building a limited capacity queue.
    *
    * @param chunkSizeHint is an optimization parameter — the underlying
    *        implementation may use an internal buffer that uses linked
    *        arrays, in which case the "chunk size" represents the size
    *        of a chunk; providing it is just a hint, it may or may not be
    *        used
    *
    * @param cs $csParam
    * @param F $concurrentParam
    */
  def unbounded[F[_], A](
    chunkSizeHint: Option[Int] = None
  )(implicit F: Concurrent[F]): F[ConcurrentQueue[F, A]] =
    withConfig(Unbounded(chunkSizeHint), MPMC)

  /**
    * Builds an [[ConcurrentQueue]] with fined tuned config parameters.
    *
    * '''UNSAFE PROTOCOL:''' This is unsafe due to problems that can happen
    * via selecting the wrong [[monix.execution.ChannelType ChannelType]],
    * so use with care.
    *
    * @param capacity $bufferCapacityParam
    * @param channelType $channelTypeDesc
    *
    * @param cs $csParam
    * @param F $concurrentParam
    */
  @UnsafeProtocol
  def withConfig[F[_], A](capacity: BufferCapacity, channelType: ChannelType)(
    implicit
    F: Concurrent[F]): F[ConcurrentQueue[F, A]] = {

    F.delay(unsafe(capacity, channelType))
  }

  /**
    * The unsafe version of the [[ConcurrentQueue.bounded]] builder.
    *
    * '''UNSAFE PROTOCOL:''' This is unsafe due to problems that can happen
    * via selecting the wrong [[monix.execution.ChannelType ChannelType]],
    * so use with care.
    *
    * '''UNSAFE BECAUSE IMPURE:''' this builder violates referential
    * transparency, as the queue keeps internal, shared state. Only use when
    * you know what you're doing, otherwise prefer [[ConcurrentQueue.withConfig]]
    * or [[ConcurrentQueue.bounded]].
    *
    * @param capacity $bufferCapacityParam
    * @param channelType $channelTypeDesc
    *
    * @param cs $csParam
    * @param F $concurrentParam
    */
  @UnsafeProtocol
  @UnsafeBecauseImpure
  def unsafe[F[_], A](capacity: BufferCapacity, channelType: ChannelType = MPMC)(
    implicit
    F: Concurrent[F]): ConcurrentQueue[F, A] = {

    new ConcurrentQueue[F, A](capacity, channelType)(F, cs)
  }

  /**
    * Returned by the [[apply]] builder.
    */
  final class ApplyBuilders[F[_]](val F: Concurrent[F]) extends AnyVal {
    /**
      * @see documentation for [[ConcurrentQueue.bounded]]
      */
    def bounded[A](capacity: Int): F[ConcurrentQueue[F, A]] =
      ConcurrentQueue.bounded(capacity)(F, cs)

    /**
      * @see documentation for [[ConcurrentQueue.unbounded]]
      */
    def unbounded[A](chunkSizeHint: Option[Int]): F[ConcurrentQueue[F, A]] =
      ConcurrentQueue.unbounded(chunkSizeHint)(F, cs)

    /**
      * @see documentation for [[ConcurrentQueue.withConfig]]
      */
    def withConfig[A](capacity: BufferCapacity, channelType: ChannelType = MPMC): F[ConcurrentQueue[F, A]] =
      ConcurrentQueue.withConfig(capacity, channelType)(F, cs)

    /**
      * @see documentation for [[ConcurrentQueue.unsafe]]
      */
    def unsafe[A](capacity: BufferCapacity, channelType: ChannelType = MPMC): ConcurrentQueue[F, A] =
      ConcurrentQueue.unsafe(capacity, channelType)(F, cs)
  }
}
