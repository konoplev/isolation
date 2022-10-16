package me.konoplev.isolation.repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import me.konoplev.isolation.PostgresTest;
import me.konoplev.isolation.repository.dto.Account;
import me.konoplev.isolation.repository.dto.User;
import me.konoplev.isolation.util.PhaseSync;
import me.konoplev.isolation.util.PhaseSync.Phases;
import me.konoplev.isolation.util.TransactionsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

@PostgresTest
public class NonRepeatableReadTest {

  @Autowired
  private AccountRepository accountRepository;

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private TransactionsWrapper transactionsWrapper;

  @PersistenceContext
  private EntityManager entityManager;

  @BeforeEach
  public void cleanUp() {
    accountRepository.deleteAll();
  }

  @Test
  public void nonRepeatableRead() {
    //given
    final var amountToTransfer = 30;
    final var firstAccountInitialAmount = 40;
    final var secondAccountInitialAmount = 50;

    var user = new User();
    user.setUserName("someName");
    var account1 = new Account();
    account1.setId(1);
    account1.setUser(user);
    account1.setAmount(firstAccountInitialAmount);
    var account2 = new Account();
    account2.setId(2);
    account2.setAmount(secondAccountInitialAmount);
    account2.setUser(user);
    user.setAccounts(List.of(account1, account2));
    userRepository.saveAndFlush(user);

    //expected
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> {
      phaseSync.phase(Phases.SECOND, () ->
          transactionsWrapper.readCommitted(() -> {
            accountRepository.updateAmount(1, firstAccountInitialAmount - amountToTransfer);
            accountRepository.updateAmount(2, secondAccountInitialAmount + amountToTransfer);
          }));
    });

    final AtomicInteger firstAccountAmount = new AtomicInteger(0);
    final AtomicInteger secondAccountAmount = new AtomicInteger(0);
    runAsync(() -> {
      transactionsWrapper.readCommitted(() -> {
        //read before another transaction started
        phaseSync.phase(Phases.FIRST, () ->
            accountRepository.findById(1).map(Account::getAmount)
                .ifPresent(amount -> firstAccountAmount.compareAndSet(0, amount)));
        //we need to clear caches, otherwise we can read cached value
        entityManager.clear();
        //read after another transaction finished
        phaseSync.phase(Phases.THIRD, () ->
            accountRepository.findById(2).map(Account::getAmount)
                .ifPresent(amount -> secondAccountAmount.compareAndSet(0, amount)));
      });
    });

    phaseSync.phase(Phases.FOURTH, () -> {/* all phases are done*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        not(firstAccountInitialAmount + secondAccountInitialAmount));

    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        is(firstAccountInitialAmount + secondAccountInitialAmount + amountToTransfer));
  }

  @Test
  public void nonRepeatableReadFix() {
    //given
    final var amountToTransfer = 30;
    final var firstAccountInitialAmount = 40;
    final var secondAccountInitialAmount = 50;

    var user = new User();
    user.setUserName("someName");
    var account1 = new Account();
    account1.setId(1);
    account1.setUser(user);
    account1.setAmount(firstAccountInitialAmount);
    var account2 = new Account();
    account2.setId(2);
    account2.setAmount(secondAccountInitialAmount);
    account2.setUser(user);
    user.setAccounts(List.of(account1, account2));
    userRepository.saveAndFlush(user);

    //expected
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> {
      phaseSync.phase(Phases.SECOND, () ->
          transactionsWrapper.readCommitted(() -> {
            accountRepository.updateAmount(1, firstAccountInitialAmount - amountToTransfer);
            accountRepository.updateAmount(2, secondAccountInitialAmount + amountToTransfer);
          }));
    });

    final AtomicInteger firstAccountAmount = new AtomicInteger(0);
    final AtomicInteger secondAccountAmount = new AtomicInteger(0);
    runAsync(() -> {
      transactionsWrapper.repeatableRead(() -> {
        //read before another transaction started
        phaseSync.phase(Phases.FIRST, () ->
            accountRepository.findById(1).map(Account::getAmount)
                .ifPresent(amount -> firstAccountAmount.compareAndSet(0, amount)));
        //we need to clear caches, otherwise we can read cached value
        entityManager.clear();
        //read after another transaction finished
        phaseSync.phase(Phases.THIRD, () ->
            accountRepository.findById(2).map(Account::getAmount)
                .ifPresent(amount -> secondAccountAmount.compareAndSet(0, amount)));
      });
    });

    phaseSync.phase(Phases.FOURTH, () -> {/* all phases are done*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        is(firstAccountInitialAmount + secondAccountInitialAmount));
  }

  @Test
  public void nonRepeatableReadWriteTransactionStartsBeforeRead() {
    //given
    final var amountToTransfer = 30;
    final var firstAccountInitialAmount = 40;
    final var secondAccountInitialAmount = 50;

    var user = new User();
    user.setUserName("someName");
    var account1 = new Account();
    account1.setId(1);
    account1.setUser(user);
    account1.setAmount(firstAccountInitialAmount);
    var account2 = new Account();
    account2.setId(2);
    account2.setAmount(secondAccountInitialAmount);
    account2.setUser(user);
    user.setAccounts(List.of(account1, account2));
    userRepository.saveAndFlush(user);

    //expected
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> {
      transactionsWrapper.readCommitted(() -> {
        phaseSync.phase(Phases.FIRST, () ->
            accountRepository.updateAmount(1, firstAccountInitialAmount - amountToTransfer));
        phaseSync.phase(Phases.FOURTH, () ->
            accountRepository.updateAmount(2, secondAccountInitialAmount + amountToTransfer));
      });
      phaseSync.phase(Phases.FIFTH, () -> {/* writing transaction is committed */});
    });

    final AtomicInteger firstAccountAmount = new AtomicInteger(0);
    final AtomicInteger secondAccountAmount = new AtomicInteger(0);
    runAsync(() -> {
      phaseSync.phase(Phases.SECOND, () -> {/* wait until writing transaction is started */});
      transactionsWrapper.readCommitted(() -> {
        //read before another transaction started
        phaseSync.phase(Phases.THIRD, () ->
            accountRepository.findById(1).map(Account::getAmount)
                .ifPresent(amount -> firstAccountAmount.compareAndSet(0, amount)));
        //we need to clear caches, otherwise we can read cached value
        entityManager.clear();
        //read after another transaction finished
        phaseSync.phase(Phases.SIXTH, () ->
            accountRepository.findById(2).map(Account::getAmount)
                .ifPresent(amount -> secondAccountAmount.compareAndSet(0, amount)));
      });
    });

    phaseSync.phase(Phases.SEVENTH, () -> {/* all phases are done*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        not(firstAccountInitialAmount + secondAccountInitialAmount));

    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        is(firstAccountInitialAmount + secondAccountInitialAmount + amountToTransfer));
  }

  @Test
  public void nonRepeatableReadWriteTransactionStartsBeforeReadFix() {
    //given
    final var amountToTransfer = 30;
    final var firstAccountInitialAmount = 40;
    final var secondAccountInitialAmount = 50;

    var user = new User();
    user.setUserName("someName");
    var account1 = new Account();
    account1.setId(1);
    account1.setUser(user);
    account1.setAmount(firstAccountInitialAmount);
    var account2 = new Account();
    account2.setId(2);
    account2.setAmount(secondAccountInitialAmount);
    account2.setUser(user);
    user.setAccounts(List.of(account1, account2));
    userRepository.saveAndFlush(user);

    //expected
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> {
      transactionsWrapper.readCommitted(() -> {
        phaseSync.phase(Phases.FIRST, () ->
            accountRepository.updateAmount(1, firstAccountInitialAmount - amountToTransfer));
        phaseSync.phase(Phases.FOURTH, () ->
            accountRepository.updateAmount(2, secondAccountInitialAmount + amountToTransfer));
      });
      phaseSync.phase(Phases.FIFTH, () -> {/* writing transaction is committed */});
    });

    final AtomicInteger firstAccountAmount = new AtomicInteger(0);
    final AtomicInteger secondAccountAmount = new AtomicInteger(0);
    runAsync(() -> {
      phaseSync.phase(Phases.SECOND, () -> {/* wait until writing transaction is started */});
      transactionsWrapper.repeatableRead(() -> {
        //read before another transaction started
        phaseSync.phase(Phases.THIRD, () ->
            accountRepository.findById(1).map(Account::getAmount)
                .ifPresent(amount -> firstAccountAmount.compareAndSet(0, amount)));
        //we need to clear caches, otherwise we can read cached value
        entityManager.clear();
        //read after another transaction finished
        phaseSync.phase(Phases.SIXTH, () ->
            accountRepository.findById(2).map(Account::getAmount)
                .ifPresent(amount -> secondAccountAmount.compareAndSet(0, amount)));
      });
    });

    phaseSync.phase(Phases.SEVENTH, () -> {/* all phases are done*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        is(firstAccountInitialAmount + secondAccountInitialAmount));
  }

}
