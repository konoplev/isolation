package me.konoplev.isolation.repository;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import me.konoplev.isolation.PostgresTest;
import me.konoplev.isolation.TransactionsWrapper;
import me.konoplev.isolation.repository.dto.Account;
import me.konoplev.isolation.repository.dto.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@PostgresTest
class DirtyReadTest {

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
    // given
    var sameName = "sameName";
    transactionsWrapper.readCommitted(() -> {
      var user = new User();
      user.setUserName(sameName);
      userRepository.save(user);
    });
    var accountIsCreatedLatch = new CountDownLatch(1);
    var accountIsFetchedLatch = new CountDownLatch(2);
    var transactionIsRolledBack = new CountDownLatch(1);
    // when
    runAsync(() -> {
      try {
        transactionsWrapper.readUncommitted(() -> {
          for (int i = 0; i < 100; i++) {
            var account = new Account();
            account.setAmount(i);
            accountRepository.saveAndFlush(account);
          }
          accountIsCreatedLatch.countDown();
          wait(accountIsFetchedLatch);
          var user = new User();
          user.setUserName("sameName");
          user.setAccounts(List.of());
          userRepository.saveAndFlush(user);
        });
      } catch (Exception e) {
        // expected
        System.out.println(e);
        transactionIsRolledBack.countDown();
      }
    });

    // then
    runAsync(() -> {
      transactionsWrapper.readUncommitted(() -> {
        wait(accountIsCreatedLatch);
        System.out.println("Number of accounts while transaction is executing: " + accountRepository.count());
        System.out.println("Number of users while transaction is executing: " + userRepository.count());
        accountIsFetchedLatch.countDown();
      });
    });
    runAsync(() -> {
      transactionsWrapper.readCommitted(() -> {
        wait(accountIsCreatedLatch);
        System.out.println("Number of accounts from read committed: " + accountRepository.count());
        accountIsFetchedLatch.countDown();
      });
    });
    wait(transactionIsRolledBack);
  }

  @Test
  public void test2() {
    //given
    AtomicInteger userIdAtomic = new AtomicInteger(-1);
    transactionsWrapper.readCommitted(() -> {
      var user = new User();
      user.setUserName("someName");
      userIdAtomic.set(userRepository.save(user).getId());
    });
    int userId = userIdAtomic.get();

    //expected
    CountDownLatch startCreating = new CountDownLatch(1);
    CountDownLatch partiallyCreated = new CountDownLatch(1);
    CountDownLatch canBeCompletedNow = new CountDownLatch(1);

    runAsync(() -> {
      transactionsWrapper.readUncommitted(() -> {
        wait(startCreating);
        var account = new Account();
        account.setAmount(10);
        accountRepository.save(account);
        assertThat(accountRepository.count(), is(1L));
        partiallyCreated.countDown();
        wait(canBeCompletedNow);
        var user = new User();
        user.setAccounts(List.of(account));
        user.setUserName("someName");
        userRepository.save(user);
      });
    });

    transactionsWrapper.readUncommitted(() -> {
      assertThat(accountRepository.count(), is(0L));

      //now another transaction runs in parallel and creates the account
      startCreating.countDown();
      wait(partiallyCreated);

      //this transaction sees that there is 1 account, but it will be reverted soon
      assertThat(accountRepository.count(), is(1L));
      canBeCompletedNow.countDown();
    });

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
