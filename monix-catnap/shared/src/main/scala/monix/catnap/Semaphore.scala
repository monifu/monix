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

import cats.effect.{ Async, CancelToken, Concurrent }
import monix.catnap.internal.AsyncUtils
import monix.execution.Callback
import monix.execution.annotations.{ UnsafeBecauseImpure, UnsafeProtocol }
import monix.execution.atomic.PaddingStrategy
import monix.execution.atomic.PaddingStrategy.NoPadding
import monix.execution.internal.GenericSemaphore
import monix.execution.internal.GenericSemaphore.Listener
import scala.concurrent.Promise

/** The `Semaphore` is an asynchronous semaphore implementation that
  * limits the parallelism on task execution.
  *
  * The following example instantiates a semaphore with a
  * maximum parallelism of 10:
  *
  * {{{
  *   import cats.implicits._
  *   import cats.effect.IO
  *
  *   // Needed for ContextShift[IO]
  *   import monix.execution.Scheduler
  *   implicit val cs = IO.contextShift(Scheduler.global)
  *
  *   // Dummies for didactic purposes
  *   case class HttpRequest()
  *   case class HttpResponse()
  *   def makeRequest(r: HttpRequest): IO[HttpResponse] = IO(???)
  *
  *   for {
  *     semaphore <- Semaphore[IO](provisioned = 10)
  *     tasks = for (_ <- 0 until 1000) yield {
  *       semaphore.withPermit(makeRequest(HttpRequest()))
  *     }
  *     // Execute in parallel; note that due to the `semaphore`
  *     // no more than 10 tasks will be allowed to execute in parallel
  *     _ <- tasks.toList.parSequence
  *   } yield ()
  * }}}
  *
  * ==Credits==
  *
  * `Semaphore` is now implementing `cats.effect.Semaphore`, deprecating
  * the old Monix `TaskSemaphore`.
  *
  * The changes to the interface and some implementation details are
  * inspired by the implementation in Cats-Effect, which was ported
  * from FS2.
  */
final class Semaphore[F[_]] private (provisioned: Long, ps: PaddingStrategy)(
  implicit
  F: Concurrent[F] OrElse Async[F],
  cs: ContextShift[F]
) extends cats.effect.std.Semaphore[F] {

  private[this] implicit val F0: Async[F] = F.unify

  /** Returns the number of permits currently available. Always non-negative.
    *
    * The protocol is unsafe, the semaphore is used in concurrent settings
    * and thus the value returned isn't stable or reliable. Use with care.
    */
  @UnsafeProtocol
  def available: F[Long] = underlying.available

  /** Obtains a snapshot of the current count. Can be negative.
    *
    * Like [[available]] when permits are available but returns the
    * number of permits callers are waiting for when there are no permits
    * available.
    */
  @UnsafeProtocol
  def count: F[Long] = underlying.count

  /** Acquires `n` permits.
    *
    * The returned effect semantically blocks until all requested permits are
    * available. Note that acquires are satisfied in strict FIFO order, so given
    * a `Semaphore[F]` with 2 permits available, an `acquireN(3)` will
    * always be satisfied before a later call to `acquireN(1)`.
    *
    * @see [[withPermit]], the preferred way to acquire and release
    * @see [[acquire]] for a version acquires a single permit
    *
    * @param n number of permits to acquire; must be >= 0
    */
  def acquireN(n: Long): F[Unit] =
    underlying.acquireN(n)

  /** Acquires a single permit. Alias for `[[acquireN]](1)`.
    *
    * @see [[withPermit]], the preferred way to acquire and release
    * @see [[acquireN]] for a version that can acquire multiple permits
    */
  override def acquire: F[Unit] = acquireN(1)

  /** Alias for `[[tryAcquireN]](1)`.
    *
    * The protocol is unsafe, because with the "try*" methods the user needs a
    * firm grasp of what race conditions are and how they manifest and usage of
    * such methods can lead to very fragile logic.
    *
    * @see [[tryAcquireN]] for the version that can acquire multiple permits
    * @see [[acquire]] for the version that can wait for acquisition
    * @see [[withPermit]] the preferred way to acquire and release
    */
  @UnsafeProtocol
  def tryAcquireN(n: Long): F[Boolean] =
    underlying.tryAcquireN(n)

  /** Alias for `[[tryAcquireN]](1)`.
    *
    * The protocol is unsafe, because with the "try*" methods the user needs a
    * firm grasp of what race conditions are and how they manifest and usage of
    * such methods can lead to very fragile logic.
    *
    * @see [[tryAcquireN]] for the version that can acquire multiple permits
    * @see [[acquire]] for the version that can wait for acquisition
    * @see [[withPermit]] the preferred way to acquire and release
    */
  @UnsafeProtocol
  override def tryAcquire: F[Boolean] = tryAcquireN(1)

  /** Releases `n` permits, potentially unblocking up to `n`
    * outstanding acquires.
    *
    * @see [[withPermit]], the preferred way to acquire and release
    *
    * @param n number of permits to release - must be >= 0
    */
  def releaseN(n: Long): F[Unit] =
    underlying.releaseN(n)

  /** Releases a permit, returning it to the pool.
    *
    * If there are consumers waiting on permits being available,
    * then the first in the queue will be selected and given
    * a permit immediately.
    *
    * @see [[withPermit]], the preferred way to acquire and release
    */
  override def release: F[Unit] = releaseN(1)

  /** Returns a new task, ensuring that the given source
    * acquires an available permit from the semaphore before
    * it is executed.
    *
    * The returned task also takes care of resource handling,
    * releasing its permit after being complete.
    *
    * @param fa is an effect to execute once the permit has been
    *        acquired; regardless of its result, the permit is
    *        released to the pool afterwards
    */
  def withPermit[A](fa: F[A]): F[A] =
    withPermitN(1)(fa)

  /** Returns a new task, ensuring that the given source
    * acquires `n` available permits from the semaphore before
    * it is executed.
    *
    * The returned task also takes care of resource handling,
    * releasing its permits after being complete.
    *
    * @param n is the number of permits required for the given
    *        function to be executed
    *
    * @param fa is an effect to execute once the permits have been
    *        acquired; regardless of its result, the permits are
    *        released to the pool afterwards
    */
  def withPermitN[A](n: Long)(fa: F[A]): F[A] =
    F0.bracket(underlying.acquireAsyncN(n)) {
      case (acquire, _) => F0.flatMap(acquire)(_ => fa)
    } {
      case (_, release) => release
    }

  /** Returns a task that will be complete when the specified
    * number of permits are available.
    *
    * The protocol is unsafe because by the time the returned
    * task completes, some other process might have already
    * acquired the available permits and thus usage of `awaitAvailable`
    * can lead to fragile concurrent logic. Use with care.
    *
    * Can be useful for termination logic, for example to execute
    * a piece of logic once all available permits have been released.
    *
    * @param n is the number of permits waited on
    */
  @UnsafeProtocol
  def awaitAvailable(n: Long): F[Unit] =
    underlying.awaitAvailable(n)

  private[this] val underlying =
    new Semaphore.Impl[F](provisioned, ps)
}

object Semaphore {
  /**
    * Builds a [[Semaphore]] instance.
    *
    * @param provisioned is the number of permits initially available
    *
    * @param ps is an optional padding strategy for avoiding the
    *        "false sharing problem", a common JVM effect when multiple threads
    *        read and write in shared variables
    *
    * @param F is the type class instance required to make `Semaphore` work,
    *        can be either `Concurrent` or `Async` for extra flexibility
    *
    * @param cs is a `ContextShift` instance required in order to introduce
    *        async boundaries after successful `acquire` operations, for safety
    */
  def apply[F[_]](provisioned: Long, ps: PaddingStrategy = NoPadding)(
    implicit
    F: Concurrent[F] OrElse Async[F]): F[Semaphore[F]] = {

    F.unify.delay(new Semaphore[F](provisioned, ps))
  }

  /** Builds a [[Semaphore]] instance.
    *
    * '''Unsafe warning:''' this violates referential transparency.
    * Use with care, prefer the pure [[Semaphore.apply]].
    *
    * @param provisioned is the number of permits initially available
    *
    * @param ps is an optional padding strategy for avoiding the
    *        "false sharing problem", a common JVM effect when multiple threads
    *        read and write in shared variables
    *
    * @param F is the type class instance required to make `Semaphore` work,
    *        can be either `Concurrent` or `Async` for extra flexibility
    *
    * @param cs is a `ContextShift` instance required in order to introduce
    *        async boundaries after successful `acquire` operations, for safety
    */
  @UnsafeBecauseImpure
  def unsafe[F[_]](provisioned: Long, ps: PaddingStrategy = NoPadding)(
    implicit
    F: Concurrent[F] OrElse Async[F]): Semaphore[F] =
    new Semaphore[F](provisioned, ps)

  implicit final class DeprecatedExtensions[F[_]](val source: Semaphore[F]) extends AnyVal {

    /**
      * DEPRECATED — renamed to [[Semaphore.withPermit withPermit]].
      *
      * Please switch to `withPermit`, as deprecated symbols will be
      * dropped in the future.
      */
    @deprecated("Renamed to: withPermit", "3.0.0")
    def greenLight[A](fa: F[A]): F[A] = source.withPermit(fa)
  }

  private final class Impl[F[_]](provisioned: Long, ps: PaddingStrategy)(
    implicit
    F: Concurrent[F] OrElse Async[F],
    F0: Async[F],
    cs: ContextShift[F]
  ) extends GenericSemaphore[F[Unit]](provisioned, ps) {

    val available: F[Long] = F0.delay(unsafeAvailable())
    val count: F[Long] = F0.delay(unsafeCount())

    def acquireN(n: Long): F[Unit] =
      F0.defer {
        if (unsafeTryAcquireN(n))
          F0.unit
        else
          F0.flatMap(make[Unit](unsafeAcquireN(n, _)))(bindFork)
      }

    def acquireAsyncN(n: Long): F[(F[Unit], CancelToken[F])] =
      F0.delay {
        // Happy path
        if (unsafeTryAcquireN(n)) {
          // This cannot be canceled in the context of `bracket`
          (F0.unit, releaseN(n))
        } else {
          val p = Promise[Unit]()
          val cancelToken = unsafeAsyncAcquireN(n, Callback.fromPromise(p))
          val acquire = FutureLift.scalaToAsync(F0.pure(p.future))
          // Extra async boundary needed for fairness
          (F0.flatMap(acquire)(bindFork), cancelToken)
        }
      }

    def tryAcquireN(n: Long): F[Boolean] =
      F0.delay(unsafeTryAcquireN(n))

    def releaseN(n: Long): F[Unit] =
      F0.delay(unsafeReleaseN(n))

    def awaitAvailable(n: Long): F[Unit] =
      F0.flatMap(make[Unit](unsafeAwaitAvailable(n, _)))(bindFork)

    protected def emptyCancelable: F[Unit] =
      F0.unit
    protected def makeCancelable(f: (Listener[Unit]) => Unit, p: Listener[Unit]): F[Unit] =
      F0.delay(f(p))

    private def make[A](k: (Either[Throwable, A] => Unit) => F[Unit]): F[A] =
      F.fold(
        F => F.cancelable(k),
        F => AsyncUtils.cancelable(k)(F)
      )

    private[this] val bindFork: (Unit => F[Unit]) =
      _ => cs.shift
  }
}
