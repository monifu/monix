/*
 * Copyright (c) 2014-2019 by The Monix Project Developers.
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

import monix.execution.misc.{CanBindLocals, Local}

/**
  * Instance of [[monix.execution.misc.CanBindLocals]] built for [[Coeval]].
  */
class CanBindLocalsForCoeval[A] extends CanBindLocals[Coeval[A]] {
  def withSuspendedContext(ctx: () => Local.Context)(f: => Coeval[A]): Coeval[A] =
    Coeval.suspend {
      val prev = Local.getContext()
      Local.setContext(ctx())
      Coeval.suspend(f).guarantee(Coeval(Local.setContext(prev)))
    }
}

object CanBindLocalsForCoeval {
  def apply[A]: CanBindLocalsForCoeval[A] = inst.asInstanceOf[CanBindLocalsForCoeval[A]]
  private[this] val inst = new CanBindLocalsForCoeval[Any]
}