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

package monix.eval.internal

import cats.effect.ExitCase
import monix.catnap.ConcurrentQueue
import monix.eval.Task
import monix.execution.{ BufferCapacity, ChannelType }
import cats.effect.Deferred

private[eval] object TaskParSequenceN {
  /**
    * Implementation for [[Task.parSequenceN]]
    */
  def apply[A](
    parallelism: Int,
    in: Iterable[Task[A]]
  ): Task[List[A]] = {
    val itemSize = in.size

    if (itemSize == 0) {
      Task.pure(List.empty)
    } else if (itemSize == 1) {
      in.head.map(List(_))
    } else {
      for {
        error <- Deferred[Task, Throwable]
        queue <- ConcurrentQueue
          .withConfig[Task, (Deferred[Task, A], Task[A])](BufferCapacity.Bounded(itemSize), ChannelType.SPMC)
        pairs <- Task.traverse(in.toList)(task => Deferred[Task, A].map(p => (p, task)))
        _     <- queue.offerMany(pairs)
        workers = Task.parSequence(List.fill(parallelism.min(itemSize)) {
          queue.poll.flatMap {
            case (p, task) =>
              task.redeemWith(
                err => error.complete(err).attempt >> Task.raiseError(err),
                p.complete
              )
          }.loopForever.start
        })
        res <- workers.bracketCase { _ =>
          Task
            .race(
              error.get,
              Task.sequence(pairs.map(_._1.get))
            )
            .flatMap {
              case Left(err) =>
                Task.raiseError(err)

              case Right(values) =>
                Task.pure(values)
            }
        } {
          case (fiber, exit) =>
            exit match {
              case ExitCase.Completed => Task.unit
              case _ => Task.traverse(fiber)(_.cancel).void
            }
        }
      } yield res
    }
  }
}
