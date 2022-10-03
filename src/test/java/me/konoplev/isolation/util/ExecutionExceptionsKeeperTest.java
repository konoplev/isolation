package me.konoplev.isolation.util;

import me.konoplev.isolation.util.PhaseSync.Phases;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExecutionExceptionsKeeperTest {

  @Test
  public void noExceptionThrownInCaseOfNoExceptions() {
    // given
    ExecutionExceptionsKeeper<Phases> keeper = new ExecutionExceptionsKeeper<>();

    // expect
    assertThat(keeper.noExceptions(), is(true));
  }

  @Test
  public void unexpectedExceptionCanBeHandledAndPrinted() {
    // given
    ExecutionExceptionsKeeper<Phases> keeper = new ExecutionExceptionsKeeper<>();

    // when
    keeper.handleUnexpectedException(Phases.FIRST, new RuntimeException("test message"));

    // then
    assertThat(keeper.noExceptions(), is(false));
    assertThat(keeper.exceptionDetails(), stringContainsInOrder(
      "Unexpected exception java.lang.RuntimeException in FIRST",
        "message: test message",
        "stack trace: ",
        "\tphases.ExecutionExceptionsKeeperTest.unexpectedExceptionCanBeHandledAndPrinted(ExecutionExceptionsKeeperTest.java:"
                                                               ));
  }

  @Test
  public void noExceptionIsRethrownIfNoExceptions() {
    // given
    ExecutionExceptionsKeeper<Phases> keeper = new ExecutionExceptionsKeeper<>();

    // when
    assertDoesNotThrow(keeper::ifAnyExceptionRethrow);
  }

  @Test
  public void unexpectedExceptionCanBeRethrown() {
    // given
    ExecutionExceptionsKeeper<Phases> keeper = new ExecutionExceptionsKeeper<>();
    keeper.handleUnexpectedException(Phases.FIRST, new RuntimeException("test message"));

    // when
    assertThrows(RuntimeException.class, keeper::ifAnyExceptionRethrow);
  }

  @Test
  public void expectedExceptionCanBeRethrown() {
    // given
    ExecutionExceptionsKeeper<Phases> keeper = new ExecutionExceptionsKeeper<>();
    keeper.handleExpectedException(Phases.FIRST, new RuntimeException("test message"), RuntimeException.class);

    // when
    assertThrows(RuntimeException.class, keeper::ifAnyExceptionRethrow);
  }

  @Test
  public void doesntRethrowThrownException() {
    // given
    ExecutionExceptionsKeeper<Phases> keeper = new ExecutionExceptionsKeeper<>();
    keeper.handleUnexpectedException(Phases.FIRST, new RuntimeException("test message"));

    // when
    try {
      keeper.ifAnyExceptionRethrow();
    } catch (Exception e) {
      // ignore. expected
    }

    // then
    assertDoesNotThrow(keeper::ifAnyExceptionRethrow);
  }

}
