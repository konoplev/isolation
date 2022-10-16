package me.konoplev.isolation.repository;

import me.konoplev.isolation.PostgresTest;
import me.konoplev.isolation.repository.dto.Account;
import me.konoplev.isolation.util.PhaseSync;
import me.konoplev.isolation.util.PhaseSync.Phases;
import me.konoplev.isolation.util.TransactionsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@PostgresTest
public class LostUpdateTest {

  @Autowired
  private AccountRepository accountRepository;

  @Autowired
  private TransactionsWrapper transactionsWrapper;

  @BeforeEach
  public void cleanUp() {
    accountRepository.deleteAll();
  }

  @Test
  public void lostUpdateTest() {
    //given
    final int userAccountId = 1;
    transactionsWrapper.readCommitted(() -> {
      var account = new Account();
      account.setAmount(0);
      account.setId(userAccountId);
      accountRepository.saveAndFlush(account);
    });

    //expected
    var phaseSync = new PhaseSync();
    var firstUserTransfer = 50;
    runAsync(() ->
            transactionsWrapper.readCommitted(() -> {
              Integer currentAmount = accountRepository.findById(userAccountId).map(Account::getAmount).orElseThrow();
              phaseSync.phase(Phases.FIRST, () -> {
                accountRepository.updateAmount(userAccountId, firstUserTransfer + currentAmount);
              });
            })
            );

    var secondUserTransfer = 30;
    runAsync(() ->
            transactionsWrapper.readCommitted(() -> {
              Integer currentAmount = accountRepository.findById(userAccountId).map(Account::getAmount).orElseThrow();
              phaseSync.phase(Phases.SECOND, () -> {
                accountRepository.updateAmount(userAccountId, secondUserTransfer + currentAmount);
              });
            })
            );

    phaseSync.phase(Phases.THIRD, () -> {/* both transactions are done */});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));

    Integer finalAmount = accountRepository.findById(userAccountId).map(Account::getAmount).orElseThrow();
    assertThat(finalAmount, not(firstUserTransfer + secondUserTransfer));
    assertThat(finalAmount, is(secondUserTransfer));

  }

  @Test
  public void lostUpdateFix() {
    //given
    final int userAccountId = 1;
    transactionsWrapper.readCommitted(() -> {
      var account = new Account();
      account.setAmount(0);
      account.setId(userAccountId);
      accountRepository.saveAndFlush(account);
    });

    //expected
    var phaseSync = new PhaseSync();
    var firstUserTransfer = 50;
    runAsync(() ->
            transactionsWrapper.repeatableRead(() -> {
              Integer currentAmount = accountRepository.findById(userAccountId).map(Account::getAmount).orElseThrow();
              phaseSync.phase(Phases.FIRST, () -> {
                accountRepository.updateAmount(userAccountId, firstUserTransfer + currentAmount);
              });
            })
            );

    var secondUserTransfer = 30;
    runAsync(() ->
            assertThrows(CannotAcquireLockException.class, () -> {
              transactionsWrapper.repeatableRead(() -> {
                Integer currentAmount = accountRepository.findById(userAccountId).map(Account::getAmount).orElseThrow();
                phaseSync.phase(Phases.SECOND, () -> {
                  accountRepository.updateAmount(userAccountId, secondUserTransfer + currentAmount);
                });
              });
            })
            );

    phaseSync.phase(Phases.THIRD, () -> {/* both transactions are done */});

    assertThat(phaseSync.noExceptions(), is(false));

    // so, this transaction is failed and will be reverted as soon as the exception we caught is re-thrown.
    assertThat(phaseSync.exceptionDetails(),
        startsWith("Unexpected exception org.springframework.dao.CannotAcquireLockException"));

    Integer finalAmount = accountRepository.findById(userAccountId).map(Account::getAmount).orElseThrow();
    assertThat(finalAmount, is(firstUserTransfer));
  }

}
