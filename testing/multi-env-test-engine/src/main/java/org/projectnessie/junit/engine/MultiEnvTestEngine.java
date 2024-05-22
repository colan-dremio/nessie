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

import static org.projectnessie.junit.engine.MultiEnvAnnotationUtils.dimensionTypeOf;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.jupiter.engine.config.CachingJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.NestedClassTestDescriptor;
import org.junit.jupiter.engine.descriptor.TestMethodTestDescriptor;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a JUnit5 Test Engine that delegates test discovery to {@link JupiterTestEngine} and
 * replicates the discovered tests for execution in multiple test environments.
 *
 * <p>Each {@link MultiEnvTestExtension} defines a dimension on which to expand. When multiple
 * {@link MultiEnvTestExtension}s are applied to the same test, the engine performs a
 * cartesian product of applied dimension types.
 *
 * <p>For example, a test class annotated with these {@link MultiEnvTestExtension}s:
 *
 * <ul>
 *   <li>ExtensionA with dimension type "dimensionA" and dimension values [1, 2, 3]
 *   <li>ExtensionB with dimension type "dimensionA" and dimension values [1, 2]
 *   <li>ExtensionC with dimension type "dimensionA" and dimension values [1]
 * </ul>
 *
 * will result in the following tests with JUnit UniqueIds:
 *
 * <ul>
 *   <li>[engine:nessie-multi-env][dimensionA:1][dimensionB:1][dimensionC:1][class:testClass]
 *   <li>[engine:nessie-multi-env][dimensionA:1][dimensionB:2][dimensionC:1][class:testClass]
 *   <li>[engine:nessie-multi-env][dimensionA:2][dimensionB:1][dimensionC:1][class:testClass]
 *   <li>[engine:nessie-multi-env][dimensionA:2][dimensionB:2][dimensionC:1][class:testClass]
 *   <li>[engine:nessie-multi-env][dimensionA:3][dimensionB:1][dimensionC:1][class:testClass]
 *   <li>[engine:nessie-multi-env][dimensionA:3][dimensionB:2][dimensionC:1][class:testClass]
 * </ul>
 *
 * <p>Actual test environments are expected to be managed by JUnit 5 extensions implementing the
 * {@link MultiEnvTestExtension} interface.
 */
public class MultiEnvTestEngine implements TestEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiEnvTestEngine.class);

  public static final String ENGINE_ID = "nessie-multi-env";

  private static final DimensionTypes ROOT_KEY = new DimensionTypes(Collections.emptyList());
  private static final MultiEnvExtensionRegistry registry = new MultiEnvExtensionRegistry();
  private static final boolean FAIL_ON_MISSING_ENVIRONMENTS =
      !Boolean.getBoolean("org.projectnessie.junit.engine.ignore-empty-environments");

  private final JupiterTestEngine delegate = new JupiterTestEngine();

  @Override
  public String getId() {
    return ENGINE_ID;
  }

  @Override
  public void execute(ExecutionRequest request) {
    delegate.execute(request);
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId) {
    try {
      TestDescriptor originalRoot = delegate.discover(discoveryRequest, uniqueId);
      List<TestDescriptor> originalChildren = new ArrayList<>(originalRoot.getChildren());

      AtomicBoolean foundAtLeastOneDimensionType = new AtomicBoolean();
      List<String> processedExtensionNames = new ArrayList<>();

      ListMultimap<DimensionTypes, TestDescriptor> nodeCache =
          MultimapBuilder.hashKeys().arrayListValues().build();
      nodeCache.put(ROOT_KEY, originalRoot);

      // Scan existing nodes for multi-env test extensions
      originalRoot.accept(
          testDescriptor -> {
            if (testDescriptor instanceof ClassTestDescriptor) {
              Class<?> testClass = ((ClassBasedTestDescriptor) testDescriptor).getTestClass();

              registry.registerExtensions(testClass);

              List<? extends MultiEnvTestExtension> orderedMultiEnvExtensionsOnTest =
                  registry.stream(testClass)
                      .sorted(
                          Comparator.comparing(MultiEnvTestExtension::segmentPriority)
                              .reversed()
                              .thenComparing(MultiEnvAnnotationUtils::dimensionTypeOf))
                      .collect(Collectors.toList());

              if (orderedMultiEnvExtensionsOnTest.isEmpty()) {
                return;
              }

              // Construct an intermediate tree of the cartesian product of applied extensions.
              // Multiple nodes will exist at a given level - one for each combination of possible
              // environment IDs (including expanding parent nodes).
              List<TestDescriptor> parentNodes;
              DimensionTypes currentPosition = ROOT_KEY;
              for (MultiEnvTestExtension extension : orderedMultiEnvExtensionsOnTest) {
                processedExtensionNames.add(extension.getClass().getSimpleName());
                parentNodes = nodeCache.get(currentPosition);
                currentPosition = currentPosition.append(dimensionTypeOf(extension));

                for (TestDescriptor parentNode : parentNodes) {
                  for (String dimensionValue :
                      extension.allDimensionValues(discoveryRequest.getConfigurationParameters())) {
                    foundAtLeastOneDimensionType.set(true);
                    UniqueId newId =
                        parentNode.getUniqueId().append(dimensionTypeOf(extension), dimensionValue);
                    MultiEnvTestDescriptor newChild =
                        new MultiEnvTestDescriptor(newId, dimensionValue);
                    parentNode.addChild(newChild);
                    nodeCache.put(currentPosition, newChild);
                  }
                }
              }

              // Add this test into each known node at the current level
              List<String> currentDimensionTypes = currentPosition.get();
              for (TestDescriptor nodeAtCurrentPosition : nodeCache.get(currentPosition)) {
                String currentDimensionValues =
                    nodeAtCurrentPosition.getUniqueId().getSegments().stream()
                        .filter(s -> currentDimensionTypes.contains(s.getType()))
                        .map(Segment::getValue)
                        .collect(Collectors.joining(","));

                putTestIntoParent(
                    testDescriptor, nodeAtCurrentPosition, currentDimensionValues, discoveryRequest);
              }
            }
          });

      // Note: this also removes the reference to parent from the child
      originalChildren.forEach(originalRoot::removeChild);

      if (FAIL_ON_MISSING_ENVIRONMENTS
          && !processedExtensionNames.isEmpty()
          && !foundAtLeastOneDimensionType.get()) {
        throw new IllegalStateException(
            String.format(
                "%s was enabled, but test extensions %s did not discover any environment IDs.",
                MultiEnvTestEngine.class.getSimpleName(),
                Arrays.toString(processedExtensionNames.toArray())));
      }

      return originalRoot;
    } catch (Exception e) {
      LOGGER.error("Failed to discover tests", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<String> getGroupId() {
    return Optional.of("org.projectnessie.nessie");
  }

  @Override
  public Optional<String> getArtifactId() {
    return Optional.of("nessie-multi-env-test-engine");
  }

  /** Immutable key of segment types for the intermediate cartesian product tree. */
  private static class DimensionTypes {

    private final List<String> components;

    public DimensionTypes(List<String> components) {
      this.components = components;
    }

    public List<String> get() {
      return new ArrayList<>(components);
    }

    public DimensionTypes append(String component) {
      List<String> newComponents = new ArrayList<>(components);
      newComponents.add(component);
      return new DimensionTypes(newComponents);
    }

    @Override
    public String toString() {
      return components.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DimensionTypes dimensionTypes = (DimensionTypes) o;
      return Objects.equals(components, dimensionTypes.components);
    }

    @Override
    public int hashCode() {
      return Objects.hash(components);
    }
  }

  private static void putTestIntoParent(
      TestDescriptor test,
      TestDescriptor parent,
      String currentDimensionValues,
      EngineDiscoveryRequest discoveryRequest) {
    JupiterConfiguration nodeConfiguration =
        new CachingJupiterConfiguration(
            new MultiEnvJupiterConfiguration(
                discoveryRequest.getConfigurationParameters(), currentDimensionValues));

    parent.addChild(nodeWithIdAsChildOf(test, parent.getUniqueId(), nodeConfiguration));
  }

  /**
   * Returns a new TestDescriptor node as if it were a child of the provided parent ID. Recursively
   * generates new children with appropriate IDs, if any.
   */
  private static TestDescriptor nodeWithIdAsChildOf(
      TestDescriptor originalNode, UniqueId parentId, JupiterConfiguration configuration) {
    UniqueId newId = parentId.append(originalNode.getUniqueId().getLastSegment());

    TestDescriptor nodeWithNewId;
    if (originalNode instanceof ClassTestDescriptor) {
      nodeWithNewId =
          new ClassTestDescriptor(
              newId, ((ClassTestDescriptor) originalNode).getTestClass(), configuration);
    } else if (originalNode instanceof NestedClassTestDescriptor) {
      nodeWithNewId =
          new NestedClassTestDescriptor(
              newId, ((NestedClassTestDescriptor) originalNode).getTestClass(), configuration);
    } else if (originalNode instanceof TestMethodTestDescriptor) {
      nodeWithNewId =
          new TestMethodTestDescriptor(
              newId,
              ((TestMethodTestDescriptor) originalNode).getTestClass(),
              ((TestMethodTestDescriptor) originalNode).getTestMethod(),
              configuration);
    } else {
      throw new IllegalArgumentException(
          String.format("Unable to process node of type %s.", originalNode.getClass().getName()));
    }

    for (TestDescriptor originalChild : originalNode.getChildren()) {
      TestDescriptor newChild = nodeWithIdAsChildOf(originalChild, newId, configuration);
      nodeWithNewId.addChild(newChild);
    }

    return nodeWithNewId;
  }
}
