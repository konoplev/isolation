package me.konoplev.isolation.repository;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import me.konoplev.isolation.MySqlTest;
import me.konoplev.isolation.repository.dto.Account;
import me.konoplev.isolation.repository.dto.User;
import me.konoplev.isolation.util.PhaseSync;
import me.konoplev.isolation.util.PhaseSync.Phases;
import me.konoplev.isolation.util.TransactionsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;

@MySqlTest
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
  public void dirtyRead() {
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

    // expect
    PhaseSync phaseSync = new PhaseSync();
    runAsync(() -> {
      transactionsWrapper.readUncommitted(() -> {
        phaseSync.phase(Phases.FIRST, () ->
                accountRepository.updateAmount(1, firstAccountInitialAmount - amountToTransfer)
                       );
        phaseSync.phase(Phases.THIRD, () ->
                accountRepository.updateAmount(2, secondAccountInitialAmount + amountToTransfer)
                       );

      });
    });

    final AtomicInteger firstAccountAmount = new AtomicInteger(0);
    final AtomicInteger secondAccountAmount = new AtomicInteger(0);
    runAsync(() -> {
      transactionsWrapper.readUncommitted(() -> {
        phaseSync.phase(Phases.SECOND, () -> {
          accountRepository.findById(1).map(Account::getAmount)
              .ifPresent(amount -> firstAccountAmount.compareAndSet(0, amount));
          accountRepository.findById(2).map(Account::getAmount)
              .ifPresent(amount -> secondAccountAmount.compareAndSet(0, amount));
        });
      });
    });

    phaseSync.phase(Phases.FOURTH, () -> {/* all phases are done*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        not(firstAccountInitialAmount + secondAccountInitialAmount));

    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        is(firstAccountInitialAmount + secondAccountInitialAmount - amountToTransfer));
  }

  @Test
  public void dirtyReadFix() {
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

    // expect
    PhaseSync phaseSync = new PhaseSync();
    runAsync(() -> {
      transactionsWrapper.readUncommitted(() -> {
        phaseSync.phase(Phases.FIRST, () ->
                accountRepository.updateAmount(1, firstAccountInitialAmount - amountToTransfer)
                       );
        phaseSync.phase(Phases.THIRD, () ->
                accountRepository.updateAmount(2, secondAccountInitialAmount + amountToTransfer)
                       );

      });
    });

    final AtomicInteger firstAccountAmount = new AtomicInteger(0);
    final AtomicInteger secondAccountAmount = new AtomicInteger(0);
    runAsync(() -> {
      transactionsWrapper.readCommitted(() -> {
        phaseSync.phase(Phases.SECOND, () -> {
          accountRepository.findById(1).map(Account::getAmount)
              .ifPresent(amount -> firstAccountAmount.compareAndSet(0, amount));
          accountRepository.findById(2).map(Account::getAmount)
              .ifPresent(amount -> secondAccountAmount.compareAndSet(0, amount));
        });
      });
    });

    phaseSync.phase(Phases.FOURTH, () -> {/* all phases are done*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(firstAccountAmount.get() + secondAccountAmount.get(),
        is(firstAccountInitialAmount + secondAccountInitialAmount));
  }

  @Test
  public void readDataThatIsRolledBack() {
    //given
    transactionsWrapper.readCommitted(() -> {
      var user = new User();
      user.setUserName("someName");
      userRepository.saveAndFlush(user);
    });

    //expected
    var phaseSync = new PhaseSync();
    runAsync(() -> {
      try {
        transactionsWrapper.readUncommittedFallible(() -> {
          //partially create an account
          var account = new Account();

          phaseSync.phase(Phases.SECOND, () -> {
            account.setAmount(10);
            account.setId(1);
            accountRepository.saveAndFlush(account);
          });

          phaseSync.phaseWithExpectedException(Phases.FOURTH, () -> {
            var user = new User();
            user.setAccounts(List.of(account));
            user.setUserName("someName");
            userRepository.saveAndFlush(user);
            //the exception is thrown because there is an account with this name already
            //so the whole transaction is reverted
          }, DataIntegrityViolationException.class);
          phaseSync.ifAnyExceptionRethrow();
        });
      } catch (Exception e) {
        phaseSync.phase(Phases.FIFTH, () -> {
          //Spring is rolling the transaction back
        });
      }
    });

    transactionsWrapper.readUncommitted(() -> {
      phaseSync.phase(Phases.FIRST, () -> {
        //there are no accounts yet
        assertThat(accountRepository.count(), is(0L));
      });

      //now another transaction runs in parallel and creates the account
      phaseSync.phase(Phases.THIRD, () -> {
        //this transaction sees that there is 1 account, but it will be reverted soon
        assertThat(accountRepository.count(), is(1L));
      });

      // the parallel transaction is rolled back. no accounts again
      phaseSync.phase(Phases.SIXTH, () -> {
        assertThat(accountRepository.count(), is(0L));
      });
    });

    phaseSync.phase(Phases.SEVENTH, () -> {/*done with all phases*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));

  }

}
