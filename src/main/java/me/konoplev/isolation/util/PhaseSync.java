package me.konoplev.isolation.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;
import java.util.function.Consumer;

public class PhaseSync {

  private final Lock lock = new ReentrantLock();
  private final Condition phaseIsDone = lock.newCondition();
  private Phases currentPhase = Phases.FIRST;
  private final ExecutionExceptionsKeeper<Phases> executionExceptionsKeeper = new ExecutionExceptionsKeeper<>();

  public void phase(Phases phase, FallibleFunction execution) {
    phase(phase, execution, (e) -> executionExceptionsKeeper.handleUnexpectedException(phase, e));
  }

  public void phaseWithExpectedException(Phases phase, FallibleFunction execution, Class<? extends Exception> expectedException) {
    AtomicBoolean theExceptionIsHandled = new AtomicBoolean(false);
    phase(
        phase,
        () -> {
          execution.run();
          if (!theExceptionIsHandled.get()) {
            executionExceptionsKeeper.handleUnexpectedException(phase, new Exception("Expected exception " + expectedException + " is not thrown"));
          }
        },
        (e) -> {
          executionExceptionsKeeper.handleExpectedException(phase, e, expectedException);
          theExceptionIsHandled.set(true);
        });
  }

  public boolean noExceptions() {
    lock.lock();
    try {
      return executionExceptionsKeeper.noExceptions();
    } finally {
      lock.unlock();
    }
  }

  public String exceptionDetails() {
    lock.lock();
    try {
      return executionExceptionsKeeper.exceptionDetails();
    } finally {
      lock.unlock();
    }
  }

  public void ifAnyExceptionRethrow() throws Exception {
    lock.lock();
    try {
      executionExceptionsKeeper.ifAnyExceptionRethrow();
    } finally {
      lock.unlock();
    }
  }

  private void executeAndHandleExceptions(FallibleFunction execution, Consumer<Exception> exceptionHandler) {
    try {
      execution.run();
    } catch (Exception e) {
      exceptionHandler.accept(e);
    }
  }

  private void phase(Phases phase, FallibleFunction execution, Consumer<Exception> exceptionHandler) {
    lock.lock();
    try {
      while (currentPhase != phase) {
        if (!phaseIsDone.await(5, TimeUnit.SECONDS)) {
          exceptionHandler.accept(new Exception("Timeout waiting for " + phase));
          return;
        }
      }
      executeAndHandleExceptions(execution, exceptionHandler);

      if (currentPhase.hasNext()) {
        currentPhase = currentPhase.next();
      }
      phaseIsDone.signalAll();
    } catch (InterruptedException e) {
      throw new RuntimeException("Thread interrupted");
    } finally {
      lock.unlock();
    }
  }

  public enum Phases {
    FIRST,
    SECOND,
    THIRD,
    FOURTH,
    FIFTH,
    SIXTH,
    SEVENTH,
    EIGHTH,
    NINTH,
    TENTH;

    public boolean hasNext() {
      return next() != null;
    }

    public Phases next() {
      return Phases.values()[ordinal() + 1];
    }
  }

  @FunctionalInterface
  public interface FallibleFunction {
    void run() throws Exception;
  }

}
