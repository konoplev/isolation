package me.konoplev.isolation.repository;

import java.util.List;

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
  public void test() throws Exception {
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
