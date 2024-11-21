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

package monix

import monix.catnap.CircuitBreaker
import monix.execution.atomic.PaddingStrategy
import monix.execution.atomic.PaddingStrategy.NoPadding

import scala.concurrent.duration.{ Duration, FiniteDuration }
import cats.effect.std.Semaphore

package object eval {

  /** DEPRECATED — moved and made generic in [[monix.catnap.CircuitBreaker]].
    *
    * Please switch to that, because the deprecated symbols will be removed.
    */
  @deprecated("Moved and made generic in monix.catnap.CircuitBreaker", "3.0.0")
  type TaskCircuitBreaker = CircuitBreaker[Task]

  /** DEPRECATED — moved and made generic in [[monix.catnap.CircuitBreaker]].
    *
    * Please switch to that, because the deprecated symbols will be removed.
    */
  @deprecated("Moved and made generic in monix.catnap.CircuitBreaker", "3.0.0")
  object TaskCircuitBreaker {
    /** DEPRECATED — please use
      * [[monix.catnap.CircuitBreaker.Builders.of CircuitBreaker[Task].of]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved and made generic in monix.catnap.CircuitBreaker", "3.0.0")
    def apply(
      maxFailures: Int,
      resetTimeout: FiniteDuration,
      exponentialBackoffFactor: Double = 1.0,
      maxResetTimeout: Duration = Duration.Inf,
      onRejected: Task[Unit] = Task.unit,
      onClosed: Task[Unit] = Task.unit,
      onHalfOpen: Task[Unit] = Task.unit,
      onOpen: Task[Unit] = Task.unit,
      padding: PaddingStrategy = NoPadding
    ): Task[CircuitBreaker[Task]] = {

      // $COVERAGE-OFF$
      CircuitBreaker[Task].of(
        maxFailures = maxFailures,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = exponentialBackoffFactor,
        maxResetTimeout = maxResetTimeout,
        onRejected = onRejected,
        onClosed = onClosed,
        onHalfOpen = onHalfOpen,
        onOpen = onOpen,
        padding = padding
      )
      // $COVERAGE-ON$
    }

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.State]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.State", "3.0.0")
    type State = CircuitBreaker.State

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.Closed]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.Closed", "3.0.0")
    type Closed = CircuitBreaker.Closed

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.Closed]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.Closed", "3.0.0")
    val Closed = {
      // $COVERAGE-OFF$
      CircuitBreaker.Closed
      // $COVERAGE-ON$
    }

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.Open]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.Open", "3.0.0")
    type Open = CircuitBreaker.Open

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.Open]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.Open", "3.0.0")
    val Open = {
      // $COVERAGE-OFF$
      CircuitBreaker.Open
      // $COVERAGE-ON$
    }

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.HalfOpen]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.HalfOpen", "3.0.0")
    type HalfOpen = CircuitBreaker.HalfOpen

    /** DEPRECATED — please use [[monix.catnap.CircuitBreaker.HalfOpen]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.CircuitBreaker.HalfOpen", "3.0.0")
    val HalfOpen = {
      // $COVERAGE-OFF$
      CircuitBreaker.HalfOpen
      // $COVERAGE-ON$
    }
  }

  /** DEPRECATED — moved and made generic in [[monix.catnap.CircuitBreaker]].
    *
    * Please switch to that, because the deprecated symbols will be removed.
    */
  @deprecated("Moved and made generic in monix.catnap.MVar", "3.0.0")
  type MVar[A] = monix.catnap.MVar[Task, A]

  /** DEPRECATED — moved and made generic in [[monix.catnap.CircuitBreaker]].
    *
    * Please switch to that, because the deprecated symbols will be removed.
    */
  @deprecated("Moved and made generic in monix.catnap.MVar", "3.0.0")
  object MVar {
    /** DEPRECATED — please use [[monix.catnap.MVar.of]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.MVar.of", "3.0.0")
    def apply[A](initial: A): Task[monix.catnap.MVar[Task, A]] = {
      // $COVERAGE-OFF$
      monix.catnap.MVar[Task].of(initial)
      // $COVERAGE-ON$
    }

    /** DEPRECATED — please use [[monix.catnap.MVar.empty]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.MVar.of", "3.0.0")
    def empty[A]: Task[monix.catnap.MVar[Task, A]] = {
      // $COVERAGE-OFF$
      monix.catnap.MVar[Task].empty()
      // $COVERAGE-ON$
    }

    /** DEPRECATED — please use [[monix.catnap.MVar.of]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.MVar.of", "3.0.0")
    def withPadding[A](initial: A, ps: PaddingStrategy): Task[monix.catnap.MVar[Task, A]] = {
      // $COVERAGE-OFF$
      monix.catnap.MVar[Task].of(initial, ps)
      // $COVERAGE-ON$
    }

    /** DEPRECATED — please use [[monix.catnap.MVar.empty]].
      *
      * Switch to the new version, because the deprecated symbols will be
      * removed.
      */
    @deprecated("Moved to monix.catnap.MVar.empty", "3.0.0")
    def withPadding[A](ps: PaddingStrategy): Task[monix.catnap.MVar[Task, A]] = {
      // $COVERAGE-OFF$
      monix.catnap.MVar[Task].empty(ps)
      // $COVERAGE-ON$
    }
  }

  /** DEPRECATED — moved and made generic in [[monix.catnap.Semaphore]].
    *
    * Please switch to that, because the deprecated symbols will be removed.
    */
  @deprecated("Moved and made generic in monix.catnap.Semaphore", "3.0.0")
  type TaskSemaphore = monix.catnap.Semaphore[Task]

  /** DEPRECATED — moved and made generic in [[monix.catnap.Semaphore]].
    *
    * Please switch to that, because the deprecated symbols will be removed.
    */
  @deprecated("Moved and made generic in monix.catnap.Semaphore", "3.0.0")
  object TaskSemaphore {
    @deprecated("Switch to monix.catnap.Semaphore.apply", "3.0.0")
    def apply(maxParallelism: Int): Task[Semaphore[Task]] =
      Semaphore[Task](maxParallelism.toLong)
  }
}
