/*
 * Copyright (c) 2014-2017 by The Monix Project Developers.
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

package monix.eval
package instances

import cats.effect.Sync
import cats.{Comonad, Eval}
import monix.eval.Coeval
import scala.util.Try

/** Specification for Cats type classes, to be implemented by
  * types that can execute synchronous computations (e.g. [[Coeval]]).
  */
trait CatsSyncInstances[F[_]] extends Sync[F] with Comonad[F]

object CatsSyncInstances {
  /** Cats type class instances for [[Coeval]]. */
  class ForCoeval extends CatsSyncInstances[Coeval] {
    override def pure[A](a: A): Coeval[A] = Coeval.now(a)
    override def delay[A](thunk: => A): Coeval[A] = Coeval.eval(thunk)
    override def suspend[A](fa: => Coeval[A]): Coeval[A] = Coeval.defer(fa)
    val unit: Coeval[Unit] = Coeval.now(())

    override def extract[A](x: Coeval[A]): A =
      x.value
    override def flatMap[A, B](fa: Coeval[A])(f: (A) => Coeval[B]): Coeval[B] =
      fa.flatMap(f)
    override def flatten[A](ffa: Coeval[Coeval[A]]): Coeval[A] =
      ffa.flatten
    override def tailRecM[A, B](a: A)(f: (A) => Coeval[Either[A, B]]): Coeval[B] =
      Coeval.tailRecM(a)(f)
    override def coflatMap[A, B](fa: Coeval[A])(f: (Coeval[A]) => B): Coeval[B] =
      Coeval.eval(f(fa))
    override def coflatten[A](fa: Coeval[A]): Coeval[Coeval[A]] =
      Coeval.now(fa)
    override def ap[A, B](ff: Coeval[(A) => B])(fa: Coeval[A]): Coeval[B] =
      for (f <- ff; a <- fa) yield f(a)
    override def map2[A, B, Z](fa: Coeval[A], fb: Coeval[B])(f: (A, B) => Z): Coeval[Z] =
      for (a <- fa; b <- fb) yield f(a, b)
    override def map[A, B](fa: Coeval[A])(f: (A) => B): Coeval[B] =
      fa.map(f)
    override def raiseError[A](e: Throwable): Coeval[A] =
      Coeval.raiseError(e)
    override def handleError[A](fa: Coeval[A])(f: (Throwable) => A): Coeval[A] =
      fa.onErrorHandle(f)
    override def handleErrorWith[A](fa: Coeval[A])(f: (Throwable) => Coeval[A]): Coeval[A] =
      fa.onErrorHandleWith(f)
    override def recover[A](fa: Coeval[A])(pf: PartialFunction[Throwable, A]): Coeval[A] =
      fa.onErrorRecover(pf)
    override def recoverWith[A](fa: Coeval[A])(pf: PartialFunction[Throwable, Coeval[A]]): Coeval[A] =
      fa.onErrorRecoverWith(pf)
    override def attempt[A](fa: Coeval[A]): Coeval[Either[Throwable, A]] =
      fa.attempt
    override def catchNonFatal[A](a: => A)(implicit ev: <:<[Throwable, Throwable]): Coeval[A] =
      Coeval.eval(a)
    override def catchNonFatalEval[A](a: Eval[A])(implicit ev: <:<[Throwable, Throwable]): Coeval[A] =
      Coeval.eval(a.value)
    override def fromTry[A](t: Try[A])(implicit ev: <:<[Throwable, Throwable]): Coeval[A] =
      Coeval.fromTry(t)
  }

  /** Reusable reference for [[ForCoeval]]. */
  object ForCoeval extends ForCoeval
}