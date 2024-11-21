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

import cats.effect._
import monix.eval.Task.Context
import monix.execution.Callback
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.TrampolinedRunnable
import org.reactivestreams.{ Publisher, Subscriber }
import monix.execution.rstreams.SingleAssignSubscription

import scala.util.control.NonFatal

private[eval] object TaskConversions {
  /**
    * Implementation for `Task#toIO`.
    */
  def toIO[A](source: Task[A])(implicit eff: ConcurrentEffect[Task]): IO[A] =
    source match {
      case Task.Now(value) => IO.pure(value)
      case Task.Error(e) => IO.raiseError(e)
      case Task.Eval(thunk) => IO(thunk())
      case _ =>
        IO.cancelable { cb =>
          toIO(eff.runCancelable(source)(r => { cb(r); IO.unit }).unsafeRunSync())
        }
    }

  /**
    * Implementation for `Task#toConcurrent`.
    */
  def toConcurrent[F[_], A](source: Task[A])(implicit F: Concurrent[F], eff: ConcurrentEffect[Task]): F[A] =
    source match {
      case Task.Now(value) => F.pure(value)
      case Task.Error(e) => F.raiseError(e)
      case Task.Eval(thunk) => F.delay(thunk())
      case _ =>
        F.cancelable { cb =>
          val token = eff.runCancelable(source)(r => { cb(r); IO.unit }).unsafeRunSync()
          toConcurrent(token)(F, eff)
        }
    }

  /**
    * Implementation for `Task#toAsync`.
    */
  def toAsync[F[_], A](source: Task[A])(implicit F: Async[F], eff: Effect[Task]): F[A] =
    source match {
      case Task.Now(value) => F.pure(value)
      case Task.Error(e) => F.raiseError(e)
      case Task.Eval(thunk) => F.delay(thunk())
      case task =>
        F.async_ { cb =>
          eff.runAsync(task)(r => { cb(r); IO.unit }).unsafeRunSync()
        }
    }

  /**
    * Implementation for `Task.from`.
    */
  def fromEffect[F[_], A](fa: F[A])(implicit F: Effect[F]): Task[A] =
    fa.asInstanceOf[AnyRef] match {
      case ref: Task[A] @unchecked => ref
      case io: IO[A] @unchecked => io.to[Task]
      case _ => fromEffect0(fa)
    }

  private def fromEffect0[F[_], A](fa: F[A])(implicit F: Effect[F]): Task[A] = {
    val start = (ctx: Context, cb: Callback[Throwable, A]) => {
      try {
        implicit val sc = ctx.scheduler
        val io = F.runAsync(fa)(new CreateCallback(null, cb))
        io.unsafeRunSync()
      } catch {
        case NonFatal(e) =>
          ctx.scheduler.reportFailure(e)
      }
    }
    Task.Async(
      start,
      trampolineBefore = false,
      trampolineAfter = false
    )
  }

  /**
    * Implementation for `Task.fromConcurrent`.
    */
  def fromConcurrentEffect[F[_], A](fa: F[A])(implicit F: ConcurrentEffect[F]): Task[A] =
    fa.asInstanceOf[AnyRef] match {
      case ref: Task[A] @unchecked => ref
      case io: IO[A] @unchecked => io.to[Task]
      case _ => fromConcurrentEffect0(fa)
    }

  /**
    * Implementation for `Task.fromReactivePublisher`.
    */
  def fromReactivePublisher[A](source: Publisher[A]): Task[Option[A]] =
    Task.cancelable0 { (scheduler, cb) =>
      val sub = SingleAssignSubscription()

      source.subscribe(new Subscriber[A] {
        private[this] var isActive = true

        def onSubscribe(s: org.reactivestreams.Subscription): Unit = {
          sub := s
          sub.request(1)
        }

        def onNext(a: A): Unit = {
          if (isActive) {
            isActive = false
            sub.cancel()
            cb.onSuccess(Some(a))
          }
        }

        def onError(e: Throwable): Unit = {
          if (isActive) {
            isActive = false
            cb.onError(e)
          } else {
            scheduler.reportFailure(e)
          }
        }

        def onComplete(): Unit = {
          if (isActive) {
            isActive = false
            cb.onSuccess(None)
          }
        }
      })

      Task(sub.cancel())
    }

  private def fromConcurrentEffect0[F[_], A](fa: F[A])(implicit F: ConcurrentEffect[F]): Task[A] = {
    val start = (ctx: Context, cb: Callback[Throwable, A]) => {
      try {
        implicit val sc = ctx.scheduler
        val conn = ctx.connection
        val cancelable = TaskConnectionRef()
        conn.push(cancelable.cancel)

        val syncIO = F.runCancelable(fa)(new CreateCallback[A](conn, cb))
        cancelable := fromEffect(syncIO.unsafeRunSync(): F[Unit])
      } catch {
        case e if NonFatal(e) =>
          ctx.scheduler.reportFailure(e)
      }
    }
    Task.Async(
      start,
      trampolineBefore = false,
      trampolineAfter = false
    )
  }

  private final class CreateCallback[A](conn: TaskConnection, cb: Callback[Throwable, A])(implicit s: Scheduler)
    extends (Either[Throwable, A] => IO[Unit]) with TrampolinedRunnable {

    private[this] var canCall = true
    private[this] var value: Either[Throwable, A] = _

    def run(): Unit = {
      if (canCall) {
        canCall = false
        if (conn ne null) conn.pop()
        cb(value)
        value = null
      }
    }

    override def apply(value: Either[Throwable, A]) = {
      this.value = value
      s.execute(this)
      IO.unit
    }
  }
}
