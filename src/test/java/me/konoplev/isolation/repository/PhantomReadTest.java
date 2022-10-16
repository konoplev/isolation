package me.konoplev.isolation.repository;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import me.konoplev.isolation.PostgresTest;
import me.konoplev.isolation.repository.dto.Account;
import me.konoplev.isolation.repository.dto.User;
import me.konoplev.isolation.util.PhaseSync;
import me.konoplev.isolation.util.PhaseSync.Phases;
import me.konoplev.isolation.util.TransactionsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  public void phantomRead() {
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


    //expect
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> {
      transactionsWrapper.repeatableRead(() -> {
        AtomicBoolean isWithdrawAllowed = new AtomicBoolean(false);
        phaseSync.phase(Phases.FIRST, () ->
            isWithdrawAllowed.compareAndSet(false, allowedToWithdraw(amountToTransfer)));
        if (isWithdrawAllowed.get()) {
          phaseSync.phase(Phases.THIRD, () -> withdraw(amountToTransfer, 1));
        }
      });
      phaseSync.phase(Phases.FOURTH, () -> {/* transaction is commited */});
    });

    runAsync(() -> {
      transactionsWrapper.repeatableRead(() -> {
        AtomicBoolean isWithdrawAllowed = new AtomicBoolean(false);
        phaseSync.phase(Phases.SECOND, () ->
            isWithdrawAllowed.compareAndSet(false, allowedToWithdraw(amountToTransfer)));
        if (isWithdrawAllowed.get()) {
          phaseSync.phase(Phases.FIFTH, () -> withdraw(amountToTransfer, 2));
        }
      });
    });
    phaseSync.phase(Phases.SIXTH, () -> {/*done with all phases*/});
    assertThat(phaseSync.noExceptions(), is(true));
    // the constraint is violated
    assertThat(accountRepository.findAll().stream().mapToInt(Account::getAmount).sum(), is(30));
  }

  @Test
  public void phantomReadFix() {
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

    //expect
    PhaseSync phaseSync = new PhaseSync();

    runAsync(() -> {
        transactionsWrapper.serializable(() -> {
          AtomicBoolean isWithdrawAllowed = new AtomicBoolean(false);
          phaseSync.phase(Phases.FIRST, () ->
              isWithdrawAllowed.compareAndSet(false, allowedToWithdraw(amountToTransfer)));
          if (isWithdrawAllowed.get()) {
            phaseSync.phase(Phases.THIRD, () -> withdraw(amountToTransfer, 1));
          }
        });
      phaseSync.phase(Phases.FOURTH, () -> {/* transaction is commited */});
    });

    runAsync(() -> {
      assertThrows(CannotAcquireLockException.class, () -> {
        transactionsWrapper.serializableFallible(() -> {
          AtomicBoolean isWithdrawAllowed = new AtomicBoolean(false);
          phaseSync.phase(Phases.SECOND, () ->
              isWithdrawAllowed.compareAndSet(false, allowedToWithdraw(amountToTransfer)));
          if (isWithdrawAllowed.get()) {
            phaseSync.phase(Phases.FIFTH, () -> withdraw(amountToTransfer, 2));
          }
          // we can't update the second account. the first transaction is committed and the data we used to check the constraint is stale now
          assertThat(phaseSync.noExceptions(), is(false));

          // so, this transaction is failed and will be reverted as soon as the exception we caught is re-thrown.
          assertThat(phaseSync.exceptionDetails(), startsWith("Unexpected exception org.springframework.dao.CannotAcquireLockException"));

          phaseSync.ifAnyExceptionRethrow();
        });
      });
    });
    phaseSync.phase(Phases.SIXTH, () -> {/*done with all phases*/});
    assertThat(phaseSync.noExceptions(), is(false));

    assertThat(accountRepository.findAll().stream().mapToInt(Account::getAmount).sum(), is(60));
  }

  private void withdraw(int moneyToWithdraw, int accountIdToWithdrawFrom) {
    Integer newAmount = accountRepository.findById(accountIdToWithdrawFrom)
        .map(Account::getAmount)
        .map(amount -> amount - moneyToWithdraw)
        .filter(amount -> amount > 0)
        .orElseThrow();
    accountRepository.updateAmount(accountIdToWithdrawFrom, newAmount);
  }
  
  private boolean allowedToWithdraw(int amount) {
    return accountRepository.findAllById(List.of(1, 2)).stream()
        .mapToInt(Account::getAmount).sum() >= amount * 3;
  }

}
