/*
 * Copyright (C) 2022 Dremio
 *
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
package org.projectnessie.junit.engine;

import java.util.List;
import org.junit.jupiter.api.extension.Extension;
import org.junit.platform.engine.ConfigurationParameters;

/**
 * Interface for JUnit5 test extensions that require running the same suite of tests in multiple
 * executions environments. For example, running the same tests for multiple versions of a Nessie
 * Client.
 *
 * <p>Implementations must declare a {@link MultiEnvDimensionType} with the name of the type of their
 * Junit UniqueID segment.
 *
 * <p>See {@link MultiEnvTestEngine}.
 */
public interface MultiEnvTestExtension extends Extension {

  /**
   * Returns a list of dimension values where the related suite of tests needs to be
   * executed. For example, a dimension of Nessie server version might have values of
   * [0.80.0, 0.81.0, 0.82.0].
   *
   * <p>The returned list should preferably have the same order of elements across different
   * invocations of this method to ensure a stable test case creation order.
   */
  List<String> allDimensionValues(ConfigurationParameters configuration);

  /**
   * Allows {@link MultiEnvTestExtension}s to define their relative ordering within a JUnit
   * UniqueId's segments. Higher numbers will appear earlier in the JUnit UniqueId. Extensions with
   * the same segment priority will be sorted alphabetically by {@link MultiEnvDimensionType}.
   */
  default int segmentPriority() {
    return 0;
  }
}
