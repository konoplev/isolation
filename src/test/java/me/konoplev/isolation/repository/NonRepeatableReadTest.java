package me.konoplev.isolation.repository;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import me.konoplev.isolation.PostgresTest;
import me.konoplev.isolation.TransactionsWrapper;
import me.konoplev.isolation.repository.dto.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.UnexpectedRollbackException;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

@PostgresTest
public class NonRepeatableReadTest {

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private AccountRepository accountRepository;

  @Autowired
  private TransactionsWrapper transactionsWrapper;

  @BeforeEach
  public void cleanUp() {
    userRepository.deleteAll();
    accountRepository.deleteAll();
  }

  @Test
  public void test() {
    //given
    AtomicInteger userIdAtomic = new AtomicInteger(-1);
    transactionsWrapper.readCommitted(() -> {
      var user = new User();
      user.setUserName("someName");
      userIdAtomic.set(userRepository.save(user).getId());
    });
    int userId = userIdAtomic.get();

    //expected
    CountDownLatch startRemoving = new CountDownLatch(1);
    CountDownLatch removingCompleted = new CountDownLatch(1);

    runAsync(() -> {
      wait(startRemoving);
      transactionsWrapper.readUncommitted(() -> userRepository.deleteAll());
      assertThat(userRepository.count(), is(0L));
      removingCompleted.countDown();
    });

    transactionsWrapper.readCommitted(() -> {
      var currentUserName = userRepository.findById(userId).map(User::getUserName).orElseThrow();
      assertThat(currentUserName, is("someName"));

      //now another transaction runs in parallel and remove the record
      startRemoving.countDown();
      wait(removingCompleted);

      // there is no such record anymore. but the transaction thinks there is
      userRepository.updateUsername(userId, currentUserName.toUpperCase(Locale.ROOT));
      // username has not been actually updated, but we're not aware of it
      assertThat(userRepository.count(), is(0L));
    });

  }

  @Test
  public void testFixed() {
    //given
    AtomicInteger userIdAtomic = new AtomicInteger(-1);
    transactionsWrapper.readCommitted(() -> {
      var user = new User();
      user.setUserName("someName");
      userIdAtomic.set(userRepository.save(user).getId());
    });
    int userId = userIdAtomic.get();

    //expected
    CountDownLatch startRemoving = new CountDownLatch(1);
    CountDownLatch removingCompleted = new CountDownLatch(1);

    runAsync(() -> {
      wait(startRemoving);
      transactionsWrapper.readUncommitted(() -> userRepository.deleteAll());
      assertThat(userRepository.count(), is(0L));
      removingCompleted.countDown();
    });

    // This is the fix: Now we use repeatable read
    assertThrowsExactly(UnexpectedRollbackException.class,
        () -> transactionsWrapper.repeatableRead(() -> {
          var currentUserName = userRepository.findById(userId).map(User::getUserName).orElseThrow();
          assertThat(currentUserName, is("someName"));

          //now another transaction runs in parallel and remove the record
          startRemoving.countDown();
          wait(removingCompleted);

          // there record is there from transaction point of view, but obviously we can't modify it.
          // so the transaction will be rolled back. we can check new state of DB and decide what to do
          assertThrowsExactly(CannotAcquireLockException.class,
              () -> userRepository.updateUsername(userId, currentUserName.toUpperCase(Locale.ROOT)));
        }));

  }

  private void wait(CountDownLatch latch) {
    try {
      if (!latch.await(3, TimeUnit.MINUTES)) {
        throw new RuntimeException("Timeout exceeded");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
