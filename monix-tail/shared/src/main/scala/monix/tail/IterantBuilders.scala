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

package monix.tail

import cats.Applicative
import cats.effect._
import monix.catnap.{ ConsumerF, ProducerF }
import monix.execution.BufferCapacity.Bounded
import monix.execution.ChannelType.MultiProducer
import monix.execution.internal.Platform.recommendedBufferChunkSize
import monix.execution.{ BufferCapacity, ChannelType }
import monix.tail.Iterant.Channel
import monix.tail.batches.{ Batch, BatchCursor }
import org.reactivestreams.Publisher

import scala.collection.immutable.LinearSeq
import scala.concurrent.duration.FiniteDuration
import cats.effect.Temporal

/**
  * [[IterantBuilders.Apply]] is a set of builders for `Iterant` returned
  * by [[Iterant.apply]]
  *
  * This is used to achieve the
  * [[https://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially-Applied Type]]
  * technique.
  *
  * So instead of having to do:
  * {{{
  *   import monix.eval.Task
  *
  *   Iterant.pure[Task, Int](1)
  * }}}
  *
  * You can do:
  * {{{
  *   Iterant[Task].pure(1)
  * }}}
  */
object IterantBuilders {
  /**
    * See the description on [[IterantBuilders]] for the purpose of this class.
    *
    * Class defined inside object due to Scala's limitations on declaring
    * `AnyVal` classes.
    */
  final class Apply[F[_]](val v: Boolean = true) extends AnyVal {
    /** Aliased builder, see documentation for [[Iterant.now]]. */
    def now[A](a: A): Iterant[F, A] =
      Iterant.now(a)

    /** Aliased builder, see documentation for [[Iterant.pure]]. */
    def pure[A](a: A): Iterant[F, A] =
      Iterant.pure(a)

    /** Aliased builder, see documentation for [[Iterant.nextS]]. */
    def nextS[A](item: A, rest: F[Iterant[F, A]]): Iterant[F, A] =
      Iterant.nextS(item, rest)

    /** Aliased builder, see documentation for [[Iterant.nextCursorS]]. */
    def nextCursorS[A](cursor: BatchCursor[A], rest: F[Iterant[F, A]]): Iterant[F, A] =
      Iterant.nextCursorS(cursor, rest)

    /** Aliased builder, see documentation for [[Iterant.nextBatchS]]. */
    def nextBatchS[A](batch: Batch[A], rest: F[Iterant[F, A]]): Iterant[F, A] =
      Iterant.nextBatchS(batch, rest)

    /** Aliased builder, see documentation for [[Iterant.suspendS]]. */
    def suspendS[A](rest: F[Iterant[F, A]]): Iterant[F, A] =
      Iterant.suspendS(rest)

    /** Aliased builder, see documentation for [[Iterant.concatS]]. */
    def concatS[A](lh: F[Iterant[F, A]], rh: F[Iterant[F, A]]): Iterant[F, A] =
      Iterant.concatS(lh, rh)

    /** Aliased builder, see documentation for [[Iterant.scopeS]]. */
    def scopeS[A, B](
      acquire: F[A],
      use: A => F[Iterant[F, B]],
      close: (A, ExitCase[Throwable]) => F[Unit]
    ): Iterant[F, B] =
      Iterant.scopeS(acquire, use, close)

    /** Aliased builder, see documentation for [[Iterant.lastS]]. */
    def lastS[A](item: A): Iterant[F, A] =
      Iterant.lastS(item)

    /** Aliased builder, see documentation for [[Iterant.haltS]]. */
    def haltS[A](e: Option[Throwable]): Iterant[F, A] =
      Iterant.haltS(e)

    /** Aliased builder, see documentation for [[Iterant.empty]]. */
    def empty[A]: Iterant[F, A] =
      Iterant.empty

    /** Aliased builder, see documentation for [[Iterant.raiseError]]. */
    def raiseError[A](ex: Throwable): Iterant[F, A] =
      Iterant.raiseError(ex)

    /** Aliased builder, see documentation for [[Iterant.suspend[F[_],A](rest* Iterant.suspend]]. */
    def suspend[A](rest: F[Iterant[F, A]]): Iterant[F, A] =
      Iterant.suspend(rest)

    // -----------------------------------------------------------------
    // -- Requiring Applicative

    /** Given a list of elements build a stream out of it. */
    def of[A](elems: A*)(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromSeq(elems)(F)

    /** Aliased builder, see documentation for [[Iterant.liftF]]. */
    def liftF[A](a: F[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.liftF(a)

    /** Aliased builder, see documentation for [[Iterant.fromArray]]. */
    def fromArray[A](xs: Array[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromArray(xs)

    /** Aliased builder, see documentation for [[Iterant.fromList]]. */
    def fromList[A](xs: LinearSeq[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromList(xs)(F)

    /** Aliased builder, see documentation for [[Iterant.fromIndexedSeq]]. */
    def fromIndexedSeq[A](xs: IndexedSeq[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromIndexedSeq(xs)(F)

    /** Aliased builder, see documentation for [[Iterant.fromSeq]]. */
    def fromSeq[A](xs: Seq[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromSeq(xs)(F)

    /** Aliased builder, see documentation for [[Iterant.fromIterable]]. */
    def fromIterable[A](xs: Iterable[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromIterable(xs)(F)

    /** Aliased builder, see documentation for [[Iterant.fromBatchCursor]]. */
    def fromBatchCursor[A](xs: BatchCursor[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromBatchCursor(xs)

    /** Aliased builder, see documentation for [[Iterant.fromBatch]]. */
    def fromBatch[A](xs: Batch[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromBatch(xs)

    /** Aliased builder, see documentation for [[Iterant.fromIterator]]. */
    def fromIterator[A](xs: Iterator[A])(implicit F: Applicative[F]): Iterant[F, A] =
      Iterant.fromIterator(xs)(F)

    /** Aliased builder, see documentation for [[Iterant.range]]. */
    def range(from: Int, until: Int, step: Int = 1)(implicit F: Applicative[F]): Iterant[F, Int] =
      Iterant.range(from, until, step)(F)

    // -----------------------------------------------------------------
    // cats.effect.Sync

    /** Aliased builder, see documentation for [[Iterant.eval]]. */
    def eval[A](a: => A)(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.eval(a)(F)

    /** Aliased builder, see documentation for [[Iterant.eval]]. */
    def delay[A](a: => A)(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.delay(a)(F)

    /** Aliased builder, see documentation for [[Iterant.resource]]. */
    def resource[A](acquire: F[A])(release: A => F[Unit])(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.resource(acquire)(release)

    /** Aliased builder, see documentation for [[Iterant.resourceCase]]. */
    def resourceCase[A](acquire: F[A])(release: (A, ExitCase[Throwable]) => F[Unit])(
      implicit F: Sync[F]
    ): Iterant[F, A] =
      Iterant.resourceCase(acquire)(release)

    /** Aliased builder, see documentation for [[Iterant.fromResource]]. */
    def fromResource[A](r: Resource[F, A])(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.fromResource(r)

    /** Aliased builder, see documentation for [[Iterant.suspend[F[_],A](fa* Iterant.suspend]]. */
    def suspend[A](fa: => Iterant[F, A])(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.suspend(fa)(F)

    /** Aliased builder, see documentation for [[Iterant.defer]]. */
    def defer[A](fa: => Iterant[F, A])(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.defer(fa)(F)

    /** Aliased builder, see documentation for [[Iterant.tailRecM]]. */
    def tailRecM[A, B](a: A)(f: A => Iterant[F, Either[A, B]])(implicit F: Sync[F]): Iterant[F, B] =
      Iterant.tailRecM(a)(f)(F)

    /** Aliased builder, see documentation for [[Iterant.fromStateAction]]. */
    def fromStateAction[S, A](f: S => (A, S))(seed: => S)(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.fromStateAction(f)(seed)

    /** Aliased builder, see documentation for [[Iterant.fromLazyStateAction]]. */
    def fromStateActionL[S, A](f: S => F[(A, S)])(seed: => F[S])(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.fromLazyStateAction(f)(seed)

    /** Aliased builder, see documentation for [[Iterant.repeat]]. */
    def repeat[A](elems: A*)(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.repeat(elems: _*)

    /** Aliased builder, see documentation for [[Iterant.repeatEval]]. */
    def repeatEval[A](thunk: => A)(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.repeatEval(thunk)

    /** Aliased builder, see documentation for [[Iterant.repeatEvalF]]. */
    def repeatEvalF[A](fa: F[A])(implicit F: Sync[F]): Iterant[F, A] =
      Iterant.repeatEvalF(fa)

    // -----------------------------------------------------------------
    // cats.effect.Async

    /** Aliased builder, see documentation for [[Iterant.never]]. */
    def never[A](implicit F: Async[F]): Iterant[F, A] =
      Iterant.suspendS(F.never)

    /**
      * Aliased builder, see documentation for
      * [[[Iterant.intervalAtFixedRate[F[_]](period* Iterant.intervalAtFixedRate]]].
      */
    def intervalAtFixedRate(period: FiniteDuration)(implicit F: Async[F], timer: Temporal[F]): Iterant[F, Long] =
      Iterant.intervalAtFixedRate(period)

    /**
      * Aliased builder, see documentation for
      * [[[Iterant.intervalAtFixedRate[F[_]](initialDelay* Iterant.intervalAtFixedRate]]].
      */
    def intervalAtFixedRate(initialDelay: FiniteDuration, period: FiniteDuration)(
      implicit
      F: Async[F],
      timer: Temporal[F]
    ): Iterant[F, Long] =
      Iterant.intervalAtFixedRate(initialDelay, period)

    /**
      * Aliased builder, see documentation for
      * [[[Iterant.intervalWithFixedDelay[F[_]](delay* Iterant.intervalAtFixedRate]]].
      */
    def intervalWithFixedDelay(delay: FiniteDuration)(implicit F: Async[F], timer: Temporal[F]): Iterant[F, Long] =
      Iterant.intervalWithFixedDelay(delay)

    /**
      * Aliased builder, see documentation for
      * [[[Iterant.intervalWithFixedDelay[F[_]](initialDelay* Iterant.intervalAtFixedRate]]].
      */
    def intervalWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration)(
      implicit
      F: Async[F],
      timer: Temporal[F]
    ): Iterant[F, Long] =
      Iterant.intervalWithFixedDelay(initialDelay, delay)

    /** Aliased builder, see documentation for [[Iterant.fromReactivePublisher]]. */
    def fromReactivePublisher[A](
      publisher: Publisher[A],
      requestCount: Int = recommendedBufferChunkSize,
      eagerBuffer: Boolean = true
    )(implicit F: Async[F]): Iterant[F, A] =
      Iterant.fromReactivePublisher(publisher, requestCount, eagerBuffer)

    /** Aliased builder, see documentation for [[Iterant.fromConsumer]]. */
    def fromConsumer[A](consumer: ConsumerF[F, Option[Throwable], A], maxBatchSize: Int = recommendedBufferChunkSize)(
      implicit F: Async[F]
    ): Iterant[F, A] =
      Iterant.fromConsumer(consumer, maxBatchSize)

    /** Aliased builder, see documentation for [[Iterant.fromChannel]]. */
    def fromChannel[A](
      channel: Channel[F, A],
      bufferCapacity: BufferCapacity = Bounded(recommendedBufferChunkSize),
      maxBatchSize: Int = recommendedBufferChunkSize
    )(implicit F: Async[F]): Iterant[F, A] =
      Iterant.fromChannel(channel, bufferCapacity, maxBatchSize)

    /** Aliased builder, see documentation for [[Iterant.channel]]. */
    def channel[A](
      bufferCapacity: BufferCapacity = Bounded(recommendedBufferChunkSize),
      maxBatchSize: Int = recommendedBufferChunkSize,
      producerType: ChannelType.ProducerSide = MultiProducer
    )(
      implicit F: Concurrent[F]
    ): F[(ProducerF[F, Option[Throwable], A], Iterant[F, A])] =
      Iterant.channel(bufferCapacity, maxBatchSize, producerType)
  }
}
