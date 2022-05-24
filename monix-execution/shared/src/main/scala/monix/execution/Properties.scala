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

package monix.execution

import monix.newtypes.TypeInfo

final case class Properties private(private val attributes: Map[TypeInfo[_], Any]) {
  
  def get[A: TypeInfo]: Option[A] = attributes.get(implicitly[TypeInfo[A]]) match {
    case Some(o) => 
      // forced asInstanceOf since the only way to insert in attributes is through withProperty
      Some(o.asInstanceOf[A])
    case _ => None
  }

  def getWithDefault[A: TypeInfo](default: A): A = attributes.getOrElse(implicitly[TypeInfo[A]], default)
    .asInstanceOf[A]

  def withProperty[A: TypeInfo](value: A): Properties = Properties(attributes + (implicitly[TypeInfo[A]] -> value))
}

object Properties {
  val empty : Properties = Properties(Map())
  def apply[A: TypeInfo](a: A): Properties = 
    empty.withProperty(a)
  def apply[A: TypeInfo, B: TypeInfo](a: A, b: B): Properties = 
    empty.withProperty(a).withProperty(b)
  def apply[A: TypeInfo, B: TypeInfo, C: TypeInfo](a: A, b: B, c: C): Properties = 
    empty.withProperty(a).withProperty(b).withProperty(c)
  def apply[A: TypeInfo, B: TypeInfo, C: TypeInfo, D: TypeInfo](a: A, b: B, c: C, d: D): Properties = 
    empty.withProperty(a).withProperty(b).withProperty(c).withProperty(d)
  def apply[A: TypeInfo, B: TypeInfo, C: TypeInfo, D: TypeInfo, E: TypeInfo](a: A, b: B, c: C, d: D, e: E): Properties =
    empty.withProperty(a).withProperty(b).withProperty(c).withProperty(d).withProperty(e)
}