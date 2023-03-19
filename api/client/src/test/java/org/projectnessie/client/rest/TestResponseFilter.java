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
package org.projectnessie.client.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBackendThrottledException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.error.NessieUnsupportedMediaTypeException;
import software.amazon.awssdk.utils.StringInputStream;

@Execution(ExecutionMode.CONCURRENT)
public class TestResponseFilter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ParameterizedTest
  @MethodSource("provider")
  void testResponseFilter(
      Status responseCode, ErrorCode errorCode, Class<? extends Exception> clazz) {
    NessieError error =
        ImmutableNessieError.builder()
            .message("test-error")
            .status(responseCode.getCode())
            .errorCode(errorCode)
            .reason(responseCode.getReason())
            .serverStackTrace("xxx")
            .build();
    try {
      ResponseCheckFilter.checkResponse(new TestResponseContext(responseCode, error), MAPPER);
    } catch (Exception e) {
      assertThat(e).isInstanceOf(clazz);
      if (e instanceof NessieServiceException) {
        assertThat(((NessieServiceException) e).getError()).isEqualTo(error);
      }
      if (e instanceof BaseNessieClientServerException) {
        assertThat(((BaseNessieClientServerException) e).getStatus()).isEqualTo(error.getStatus());
        assertThat(((BaseNessieClientServerException) e).getServerStackTrace())
            .isEqualTo(error.getServerStackTrace());
      }
    }
  }

  @Test
  void testBadReturn() {
    NessieError error =
        ImmutableNessieError.builder().message("unknown").status(415).reason("xxx").build();
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new TestResponseContext(Status.UNSUPPORTED_MEDIA_TYPE, error), MAPPER))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("xxx (HTTP/415): unknown");
  }

  @Test
  void testBadReturnNoError() throws Exception {
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new ResponseContext() {
                      @Override
                      public Status getResponseCode() {
                        return Status.UNAUTHORIZED;
                      }

                      @Override
                      public InputStream getInputStream() {
                        Assertions.fail();
                        return null;
                      }

                      @Override
                      public InputStream getErrorStream() {
                        return new StringInputStream("this will fail");
                      }

                      @Override
                      public boolean isJsonCompatibleResponse() {
                        return true;
                      }

                      @Override
                      public String getContentType() {
                        return null;
                      }

                      @Override
                      public URI getRequestedUri() {
                        return null;
                      }
                    },
                    MAPPER))
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("" + Status.UNAUTHORIZED.getCode())
        .hasMessageContaining(Status.UNAUTHORIZED.getReason())
        .hasMessageContaining("JsonParseException"); // from parsing `this will fail`
  }

  @Test
  void testUnexpectedError() throws Exception {
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new ResponseContext() {
                      @Override
                      public Status getResponseCode() {
                        return Status.NOT_IMPLEMENTED;
                      }

                      @Override
                      public InputStream getInputStream() {
                        Assertions.fail();
                        return null;
                      }

                      @Override
                      public InputStream getErrorStream() {
                        // Quarkus may sometimes produce JSON error responses like this
                        return new StringInputStream(
                            "{\"details\":\"Error id ee7f7293-67ad-42bd-8973-179801e7120e-1\",\"stack\":\"\"}");
                      }

                      @Override
                      public boolean isJsonCompatibleResponse() {
                        return true;
                      }

                      @Override
                      public String getContentType() {
                        return null;
                      }

                      @Override
                      public URI getRequestedUri() {
                        return null;
                      }
                    },
                    MAPPER))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("" + Status.NOT_IMPLEMENTED.getCode())
        .hasMessageContaining(Status.NOT_IMPLEMENTED.getReason())
        .hasMessageContaining("ee7f7293-67ad-42bd-8973-179801e7120e-1")
        .hasMessageContaining("UnrecognizedPropertyException"); // jackson parse error
  }

  @Test
  void testBadReturnBadError() {
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new TestResponseContext(Status.UNAUTHORIZED, null), MAPPER))
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("" + Status.UNAUTHORIZED.getCode())
        .hasMessageContaining(Status.UNAUTHORIZED.getReason())
        .hasMessageContaining("Could not parse error object in response");
  }

  @Test
  void testGood() {
    assertDoesNotThrow(
        () -> ResponseCheckFilter.checkResponse(new TestResponseContext(Status.OK, null), MAPPER));
  }

  private static Stream<Arguments> provider() {
    return Stream.of(
        Arguments.of(Status.BAD_REQUEST, ErrorCode.UNKNOWN, RuntimeException.class),
        Arguments.of(Status.BAD_REQUEST, ErrorCode.BAD_REQUEST, NessieBadRequestException.class),
        Arguments.of(Status.UNAUTHORIZED, ErrorCode.UNKNOWN, NessieNotAuthorizedException.class),
        Arguments.of(Status.FORBIDDEN, ErrorCode.FORBIDDEN, NessieForbiddenException.class),
        Arguments.of(Status.FORBIDDEN, ErrorCode.UNKNOWN, NessieServiceException.class),
        Arguments.of(Status.TOO_MANY_REQUESTS, ErrorCode.UNKNOWN, NessieServiceException.class),
        Arguments.of(
            Status.TOO_MANY_REQUESTS,
            ErrorCode.TOO_MANY_REQUESTS,
            NessieBackendThrottledException.class),
        Arguments.of(
            Status.NOT_FOUND, ErrorCode.CONTENT_NOT_FOUND, NessieContentNotFoundException.class),
        Arguments.of(
            Status.NOT_FOUND,
            ErrorCode.REFERENCE_NOT_FOUND,
            NessieReferenceNotFoundException.class),
        Arguments.of(Status.NOT_FOUND, ErrorCode.UNKNOWN, RuntimeException.class),
        Arguments.of(Status.CONFLICT, ErrorCode.REFERENCE_CONFLICT, NessieConflictException.class),
        Arguments.of(Status.CONFLICT, ErrorCode.UNKNOWN, RuntimeException.class),
        Arguments.of(
            Status.UNSUPPORTED_MEDIA_TYPE,
            ErrorCode.UNSUPPORTED_MEDIA_TYPE,
            NessieUnsupportedMediaTypeException.class),
        Arguments.of(
            Status.INTERNAL_SERVER_ERROR, ErrorCode.UNKNOWN, NessieInternalServerException.class));
  }

  private static class TestResponseContext implements ResponseContext {

    private final Status code;
    private final NessieError error;

    TestResponseContext(Status code, NessieError error) {
      this.code = code;
      this.error = error;
    }

    @Override
    public Status getResponseCode() {
      return code;
    }

    @Override
    public InputStream getInputStream() {
      Assertions.fail();
      return null;
    }

    @Override
    public InputStream getErrorStream() throws IOException {
      if (error == null) {
        return null;
      }
      String value = MAPPER.writeValueAsString(error);
      return new StringInputStream(value);
    }

    @Override
    public boolean isJsonCompatibleResponse() {
      return true;
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public URI getRequestedUri() {
      return null;
    }
  }
}