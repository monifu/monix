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
package internal

import cats.effect.Concurrent
import monix.execution.CancelablePromise
import monix.execution.atomic.AtomicAny
import monix.execution.internal.Constants
import scala.annotation.tailrec

private[catnap] class QueueHelpers[F[_]](implicit F: Concurrent[F], cs: ContextShift[F]) {

  private[this] val asyncBoundary: F[Unit] = cs.shift

  @tailrec
  final def sleepThenRepeat[T, U](
    state: AtomicAny[CancelablePromise[Unit]],
    f: () => T,
    filter: T => Boolean,
    map: T => U,
    cb: Either[Throwable, U] => Unit
  )(implicit F: Concurrent[F]): F[Unit] = {

    // Registering intention to sleep via promise
    state.get() match {
      case null =>
        val ref = CancelablePromise[Unit]()
        if (!state.compareAndSet(null, ref))
          sleepThenRepeat(state, f, filter, map, cb)
        else
          sleepThenRepeat_Step2TryAgainThenSleep(state, f, filter, map, cb)(ref)

      case ref =>
        sleepThenRepeat_Step2TryAgainThenSleep(state, f, filter, map, cb)(ref)
    }
  }

  private final def sleepThenRepeat_Step2TryAgainThenSleep[T, U](
    state: AtomicAny[CancelablePromise[Unit]],
    f: () => T,
    filter: T => Boolean,
    map: T => U,
    cb: Either[Throwable, U] => Unit
  )(p: CancelablePromise[Unit])(implicit F: Concurrent[F]): F[Unit] = {

    // Async boundary, for fairness reasons; also creates a full
    // memory barrier between the promise registration and what follows
    F.flatMap(asyncBoundary) { _ =>
      // Trying to read one more time
      val value = f()
      if (filter(value)) {
        cb(Right(map(value)))
        F.unit
      } else {
        // Awaits on promise, then repeats
        F.flatMap(awaitPromise(p))(_ => sleepThenRepeat_Step3Awaken(state, f, filter, map, cb))
      }
    }
  }

  private final def sleepThenRepeat_Step3Awaken[T, U](
    state: AtomicAny[CancelablePromise[Unit]],
    f: () => T,
    filter: T => Boolean,
    map: T => U,
    cb: Either[Throwable, U] => Unit
  )(implicit F: Concurrent[F]): F[Unit] = {

    // Trying to read
    val value = f()
    if (filter(value)) {
      cb(Right(map(value)))
      F.unit
    } else {
      // Go to sleep again
      sleepThenRepeat(state, f, filter, map, cb)
    }
  }

  final def awaitPromise(p: CancelablePromise[Unit]): F[Unit] =
    F.cancelable { cb =>
      val token = p.subscribe(_ => cb(Constants.eitherOfUnit))
      F.delay(token.cancel())
    }
}
