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
package org.projectnessie.gc.contents.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

public class ITPostgresPersistenceSpi extends AbstractJdbcPersistenceSpi {

  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void createDataSource() throws Exception {

    String version = System.getProperty("it.nessie.container.postgres.tag", "latest");
    container = new PostgreSQLContainer<>("postgres:" + version);
    container.start();

    initDataSource(container.getJdbcUrl());
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }
}
