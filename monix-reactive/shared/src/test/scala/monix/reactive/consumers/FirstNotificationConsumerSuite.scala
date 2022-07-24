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

package monix.reactive.consumers

import cats.effect.IO
import monix.execution.BaseTestSuite

import monix.execution.exceptions.DummyException
import monix.reactive.Notification.{ OnComplete, OnError, OnNext }
import monix.reactive.{ Consumer, Observable }
import scala.util.Success

class FirstNotificationConsumerSuite extends BaseTestSuite {

  fixture.test("stops on first on next") { implicit s =>
    var wasStopped = false
    val obs = Observable.now(1).doOnEarlyStopF { () =>
      wasStopped = true
    }
    val f = obs.consumeWith(Consumer.firstNotification).runToFuture

    s.tick()
    assert(wasStopped, "wasStopped")
    assertEquals(f.value, Some(Success(OnNext(1))))
  }

  fixture.test("on complete") { implicit s =>
    var wasStopped = false
    var wasCompleted = false
    val obs = Observable
      .empty[Int]
      .doOnEarlyStopF { () =>
        wasStopped = true
      }
      .doOnCompleteF { () =>
        wasCompleted = true
      }

    val f = obs.consumeWith(Consumer.firstNotification).runToFuture

    s.tick()
    assert(!wasStopped, "!wasStopped")
    assert(wasCompleted, "wasCompleted")
    assertEquals(f.value, Some(Success(OnComplete)))
  }

  fixture.test("on error") { implicit s =>
    val ex = DummyException("dummy")
    var wasStopped = false
    var wasCompleted = false
    val obs = Observable
      .raiseError(ex)
      .doOnEarlyStopF { () =>
        wasStopped = true
      }
      .doOnErrorF { _ =>
        IO { wasCompleted = true }
      }

    val f = obs.consumeWith(Consumer.firstNotification).runToFuture

    s.tick()
    assert(!wasStopped, "!wasStopped")
    assert(wasCompleted, "wasCompleted")
    assertEquals(f.value, Some(Success(OnError(ex))))
  }
}
