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
package internal

import cats.implicits._
import cats.effect.{ Concurrent, Resource }
import monix.catnap.{ ConcurrentChannel, ConsumerF }
import monix.execution.ChannelType.SingleProducer
import monix.tail.Iterant.Consumer

private[tail] object IterantConsume {
  /**
    * Implementation for [[Iterant.consume]].
    */
  def apply[F[_], A](self: Iterant[F, A], cfg: ConsumerF.Config)(
    implicit F: Concurrent[F]
  ): Resource[F, Consumer[F, A]] = {

    /*_*/
    val res = Resource.apply {
      val channel = ConcurrentChannel.withConfig[F, Option[Throwable], A](
        producerType = SingleProducer
      )
      F.flatMap(channel) { channel =>
        val produce = self.pushToChannel(channel)
        val latch = channel.awaitConsumers(1)
        val fiber = F.start(F.flatMap(latch)(_ => produce))

        F.map(fiber) { fiber =>
          (channel.consumeWithConfig(cfg), fiber.cancel)
        }
      }
    }
    res.flatten
    /*_*/
  }
}
