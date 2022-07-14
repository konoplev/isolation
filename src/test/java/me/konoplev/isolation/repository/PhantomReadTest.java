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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static java.util.concurrent.CompletableFuture.runAsync;

@PostgresTest
public class PhantomReadTest {

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
      var account1 = new Account();
      account1.setUser(user);
      account1.setAmount(100);
      var account2 = new Account();
      account2.setAmount(50);
      account2.setUser(user);
      user.setAccounts(List.of(account1, account2));
      userIdAtomic.set(userRepository.save(user).getId());
    });
    int userId = userIdAtomic.get();

    //expected
    CountDownLatch startFirst = new CountDownLatch(1);
    CountDownLatch startSecond = new CountDownLatch(1);
    CountDownLatch bothTransactionsAreDone = new CountDownLatch(2);
    runAsync(() -> {
      transactionsWrapper.repeatableRead(() -> {
        var user = userRepository.findById(userId).orElseThrow();
        startSecond.countDown();
        wait(startFirst);
        int moneyToWithdraw = 50;
        if (getSum(user) >= moneyToWithdraw * 3) {
          var fistAccount = user.getAccounts().get(0);
          fistAccount.setAmount(fistAccount.getAmount() - moneyToWithdraw);
          userRepository.save(user);
        }
        bothTransactionsAreDone.countDown();
      });

    });
    runAsync(() -> {
      transactionsWrapper.repeatableRead(() -> {
        var user = userRepository.findById(userId).orElseThrow();
        startFirst.countDown();
        wait(startSecond);
        int moneyToWithdraw = 50;
        if (getSum(user) >= moneyToWithdraw * 3) {
          var secondAccount = user.getAccounts().get(1);
          secondAccount.setAmount(secondAccount.getAmount() - moneyToWithdraw);
          userRepository.save(user);
        }
        bothTransactionsAreDone.countDown();
      });
    });
    wait(bothTransactionsAreDone);
    assertThat(getSum(userRepository.findById(userId).orElseThrow()), is(50));
  }

  @Test
  public void testFix() {
    //given
    AtomicInteger userIdAtomic = new AtomicInteger(-1);
    transactionsWrapper.readCommitted(() -> {
      var user = new User();
      user.setUserName("someName");
      var account1 = new Account();
      account1.setUser(user);
      account1.setAmount(100);
      var account2 = new Account();
      account2.setAmount(50);
      account2.setUser(user);
      user.setAccounts(List.of(account1, account2));
      userIdAtomic.set(userRepository.save(user).getId());
    });
    int userId = userIdAtomic.get();

    //expected
    CountDownLatch startFirst = new CountDownLatch(1);
    CountDownLatch startSecond = new CountDownLatch(1);
    CountDownLatch bothTransactionsAreDone = new CountDownLatch(2);
    runAsync(() -> {
      transactionsWrapper.serializable(() -> {
        var user = userRepository.findById(userId).orElseThrow();
        startSecond.countDown();
        wait(startFirst);
        int moneyToWithdraw = 50;
        if (getSum(user) >= moneyToWithdraw * 3) {
          var fistAccount = user.getAccounts().get(0);
          fistAccount.setAmount(fistAccount.getAmount() - moneyToWithdraw);
          userRepository.save(user);
        }
        bothTransactionsAreDone.countDown();
      });

    });
    runAsync(() -> {
      transactionsWrapper.serializable(() -> {
        var user = userRepository.findById(userId).orElseThrow();
        startFirst.countDown();
        wait(startSecond);
        int moneyToWithdraw = 50;
        if (getSum(user) >= moneyToWithdraw * 3) {
          var secondAccount = user.getAccounts().get(1);
          secondAccount.setAmount(secondAccount.getAmount() - moneyToWithdraw);
          userRepository.save(user);
        }
        bothTransactionsAreDone.countDown();
      });
    });
    wait(bothTransactionsAreDone);
    //one of the transactions is rolled back
    assertThat(getSum(userRepository.findById(userId).orElseThrow()), is(100));
  }

  private int getSum(User user) {
    return user.getAccounts().stream().mapToInt(Account::getAmount).sum();
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
