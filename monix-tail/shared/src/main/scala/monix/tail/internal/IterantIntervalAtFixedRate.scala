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

package monix.tail.internal

import cats.syntax.all._
import cats.effect.{ Async, Clock }
import monix.tail.Iterant
import monix.tail.Iterant.Suspend
import scala.concurrent.duration._
import cats.effect.Temporal

private[tail] object IterantIntervalAtFixedRate {
  /**
    * Implementation for `Iterant.intervalAtFixedRate`
    */
  def apply[F[_]](
    initialDelay: FiniteDuration,
    interval: FiniteDuration
  )(implicit F: Async[F], timer: Temporal[F], clock: Clock[F]): Iterant[F, Long] = {

    def loop(time: F[Long], index: Long): F[Iterant[F, Long]] =
      time.map { startTime =>
        val rest = time.flatMap { endTime =>
          val elapsed = (endTime - startTime).nanos
          val timespan = interval - elapsed

          if (timespan > Duration.Zero) {
            F.flatMap(timer.sleep(timespan))(_ => loop(time, index + 1))
          } else {
            loop(time, index + 1)
          }
        }
        Iterant.nextS[F, Long](index, rest)
      }

    val time = clock.monotonic(NANOSECONDS)
    initialDelay match {
      case Duration.Zero =>
        Suspend(loop(time, 0))
      case _ =>
        Suspend(F.flatMap(timer.sleep(initialDelay))(_ => loop(time, 0)))
    }
  }
}
