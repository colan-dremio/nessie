/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned;

import javax.annotation.Nonnull;
import org.immutables.value.Value;

/** A named reference representing a branch. */
@Value.Immutable
public interface BranchName extends NamedRef {

  /**
   * Create a new branch reference.
   *
   * @param name the branch name
   * @return an instance of {@code BranchName} for the provided name
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  static BranchName of(@Nonnull @jakarta.annotation.Nonnull String name) {
    return ImmutableBranchName.builder().name(name).build();
  }
}
