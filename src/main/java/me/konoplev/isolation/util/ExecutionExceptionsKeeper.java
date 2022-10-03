package me.konoplev.isolation.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecutionExceptionsKeeper<Identifier> {
  private final StringBuilder exceptions = new StringBuilder();
  private Exception toRethrow;
  ExecutionExceptionsKeeper() {
  }

  public void handleExpectedException(Identifier identifier, Exception happenedException, Class<? extends Exception> expectedException) {
    if (happenedException.getClass() != expectedException) {
      handleUnexpectedException(identifier, happenedException);
    }
    setExceptionToThrowLater(happenedException);
  }

  public void handleUnexpectedException(Identifier identifier, Exception e) {
    exceptions.append("Unexpected exception ").append(e.getClass().getName()).append(" in ").append(identifier).append("\n")
        .append("message: ").append(e.getMessage()).append("\n")
        .append("stack trace: \n").append(convertStackTraceToString(e.getStackTrace())).append("\n");
    setExceptionToThrowLater(e);
  }

  public void ifAnyExceptionRethrow() throws Exception {
    if (toRethrow != null) {
      try {
        throw toRethrow;
      } finally {
        // reset toRethrow to null to prevent double throwing
        toRethrow = null;
      }
    }
  }

  private void setExceptionToThrowLater(Exception happenedException) {
    if (toRethrow == null) {
      toRethrow = happenedException;
    }
    toRethrow = happenedException;
  }

  public boolean noExceptions() {
    return exceptions.length() == 0;
  }

  public String exceptionDetails() {
    return exceptions.toString();
  }

  private String convertStackTraceToString(StackTraceElement[] stackTrace) {
    String tab = "\t";
    return tab + Stream.of(stackTrace).map(StackTraceElement::toString).collect(Collectors.joining("\n" + tab));
  }

}
