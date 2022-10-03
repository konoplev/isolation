package me.konoplev.isolation.util;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
public class TransactionsWrapper {

  @Transactional(isolation = Isolation.SERIALIZABLE, propagation = Propagation.REQUIRES_NEW)
  public void serializableFallible(FallibleFunction execute) throws Exception {
    execute.run();
  }

  @Transactional(isolation = Isolation.SERIALIZABLE, propagation = Propagation.REQUIRES_NEW)
  public void serializable(Runnable execute) {
    execute.run();
  }

  @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
  public void repeatableReadFallible(FallibleFunction execute) throws Exception {
    execute.run();
  }

  @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
  public void repeatableRead(Runnable execute) {
    execute.run();
  }

  @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
  public void readCommitted(Runnable execute) {
    execute.run();
  }

  @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
  public void readUncommittedFallible(FallibleFunction execute) throws Exception {
    execute.run();
  }

  @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
  public void readUncommitted(Runnable execute) {
    execute.run();
  }

  @FunctionalInterface
  public interface FallibleFunction {
    void run() throws Exception;
  }

}
