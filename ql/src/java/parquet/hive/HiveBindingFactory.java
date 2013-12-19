/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hive;

import parquet.hive.internal.Hive012Binding;

/**
 * Factory for creating HiveBinding objects based on the version of Hive
 * available in the classpath. This class does not provide static methods
 * to enable mocking.
 */
public class HiveBindingFactory {

  /**
   * @return HiveBinding based on the Hive version in the classpath
   */
  public HiveBinding create() {
    return new Hive012Binding();
  }
}
