/*
 * Copyright (c) 2014-2021 by The Monix Project Developers.
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

package monix.reactive.internal.operators

import monix.execution.Ack
import monix.execution.Ack.{ Continue, Stop }
import monix.reactive.Observable.Operator
import monix.reactive.observers.Subscriber

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.control.NonFatal

private[reactive] final class ConcatMapIterableOperator[-A, +B](f: A => immutable.Iterable[B]) extends Operator[A, B] {

  def apply(out: Subscriber[B]): Subscriber[A] = {
    new Subscriber[A] {
      implicit val scheduler = out.scheduler
      private[this] var isDone = false
      private[this] var ack: Future[Ack] = Continue

      def onNext(elem: A): Future[Ack] = {
        // Protects calls to user code from within the operator and
        // stream the error downstream if it happens, but if the
        // error happens because of calls to `onNext` or other
        // protocol calls, then the behavior should be undefined.
        var streamError = true
        try {
          val next = f(elem)
          streamError = false
          ack = out.onNextAll(next)
          ack
        } catch {
          case NonFatal(ex) if streamError =>
            onError(ex)
            ack = Stop
            ack
        }
      }

      def onError(ex: Throwable): Unit =
        if (!isDone) {
          isDone = true
          out.onError(ex)
        }

      def onComplete(): Unit = {
        if (!isDone) {
          isDone = true
          ack.syncOnComplete(_ => out.onComplete())
        }
      }
    }
  }
}
