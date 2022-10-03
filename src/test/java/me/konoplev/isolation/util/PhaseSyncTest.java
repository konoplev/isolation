package me.konoplev.isolation.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;

import me.konoplev.isolation.util.PhaseSync.Phases;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;

class PhaseSyncTest {

  @Test
  void usageExample() {
    PhaseSync phaseSync = new PhaseSync();

    final AtomicReference<String> stages = new AtomicReference<>("");
    BinaryOperator<String> append = (a, b) -> a + b;
    runAsync(() -> {
      phaseSync.phase(Phases.SECOND, () -> stages.getAndAccumulate("2", append));
      phaseSync.phase(Phases.FOURTH, () -> stages.getAndAccumulate("4", append));
    });
    runAsync(() -> {
      phaseSync.phase(Phases.FIRST, () -> stages.getAndAccumulate("1", append));
      phaseSync.phase(Phases.THIRD, () -> stages.getAndAccumulate("3", append));
    });
    phaseSync.phase(Phases.FIFTH, () -> {});
    assertEquals("1234", stages.get());
  }

  @Test
  void exceptionsCanBeChecked() {
    // when
    PhaseSync phaseSync = new PhaseSync();
    runAsync(
        () -> phaseSync.phase(Phases.SECOND, () -> {throw new IllegalStateException("test exception");})
            );

    phaseSync.phase(Phases.FIRST, () -> {});
    phaseSync.phase(Phases.THIRD, () -> {});

    // then
    assertThat(phaseSync.noExceptions(), is(false));
    assertThat(phaseSync.exceptionDetails(),
        containsString("Unexpected exception java.lang.IllegalStateException in SECOND"));
  }

  @Test
  void phaseCanBeExecutedWithExpectedException() {
    // when
    PhaseSync phaseSync = new PhaseSync();
    runAsync(
        () -> phaseSync.phaseWithExpectedException(
            Phases.SECOND,
            () -> {throw new IllegalStateException("test exception");},
            IllegalStateException.class
                                                  )
            );
    phaseSync.phase(Phases.FIRST, () -> {});
    phaseSync.phase(Phases.THIRD, () -> {});

    // then
    assertThat(phaseSync.noExceptions(), is(true));
  }

  @Test
  public void canRethrowException() {
    // when
    PhaseSync phaseSync = new PhaseSync();
    runAsync(
        () -> phaseSync.phaseWithExpectedException(
            Phases.SECOND,
            () -> {throw new IllegalStateException("test exception");},
            IllegalStateException.class
                                                  )
            );
    phaseSync.phase(Phases.FIRST, () -> {});
    phaseSync.phase(Phases.THIRD, () -> {});

    // then
    assertThrows(IllegalStateException.class, phaseSync::ifAnyExceptionRethrow);
  }

  @Test
  public void exceptionCanBeHandledWhenItsConvenient() {
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> phaseSync.phase(Phases.FIRST, () -> Files.readAllLines(Paths.get("/notExistingFile"))));

    phaseSync.phase(Phases.SECOND, () -> assertThrows(IOException.class, phaseSync::ifAnyExceptionRethrow));

    phaseSync.phase(Phases.THIRD, () -> Files.readAllLines(Paths.get("/dev/null")));

    runAsync(() -> phaseSync.phase(Phases.FOURTH, () -> assertDoesNotThrow(phaseSync::ifAnyExceptionRethrow)));

    phaseSync.phase(Phases.FIFTH, () -> {});

  }

  @Test
  public void exceptionIsThrownIfThereIsNoPreviousPhase() {
    // given
    PhaseSync phaseSync = new PhaseSync();

    // when
    phaseSync.phase(Phases.SECOND, () -> {});

    // then
    assertThat(phaseSync.noExceptions(), is(false));
  }

  @Test
  public void exceptionIsThrownIfPhasesAreSetInAWrongOrderInTheSameThread() {
    // given
    PhaseSync phaseSync = new PhaseSync();

    // when
    phaseSync.phase(Phases.SECOND, () -> {});
    phaseSync.phase(Phases.FIRST, () -> {});

    // then
    assertThat(phaseSync.noExceptions(), is(false));
  }

}
