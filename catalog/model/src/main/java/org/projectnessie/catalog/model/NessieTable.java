/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.model;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Represents a table in the Nessie catalog.
 *
 * <p>Table {@link org.projectnessie.model.ContentKey content-key} (name and namespace) are not
 * represented here, because the same table can be referenced using different content-keys on
 * different Nessie references (branches/tags/commits).
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieTable.class)
@JsonDeserialize(as = ImmutableNessieTable.class)
@JsonTypeName("TABLE")
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface NessieTable extends NessieEntity {

  @Override
  @Value.NonAttribute
  default String type() {
    return "TABLE";
  }

  static Builder builder() {
    return ImmutableNessieTable.builder();
  }

  @SuppressWarnings("unused")
  interface Builder extends NessieEntity.Builder<Builder> {
    @CanIgnoreReturnValue
    Builder from(NessieTable instance);

    NessieTable build();
  }
}
