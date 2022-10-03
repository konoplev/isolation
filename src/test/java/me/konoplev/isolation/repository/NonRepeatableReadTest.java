package me.konoplev.isolation.repository;

import java.util.concurrent.atomic.AtomicInteger;

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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

@PostgresTest
public class NonRepeatableReadTest {

  @Autowired
  private AccountRepository accountRepository;

  @Autowired
  private TransactionsWrapper transactionsWrapper;

  @BeforeEach
  public void cleanUp() {
    accountRepository.deleteAll();
  }

  @Test
  public void test() {
    //given
    final int userAccountId = 1;
    final int systemAccountId = 2;
    final int amount = 100;
    transactionsWrapper.readCommitted(() -> {
      var account = new Account();
      account.setAmount(amount);
      account.setId(userAccountId);
      accountRepository.saveAndFlush(account);
      var systemAccount = new Account();
      systemAccount.setAmount(0);
      systemAccount.setId(systemAccountId);
      accountRepository.saveAndFlush(systemAccount);
    });
    final var fee = 10;

    //expected
    var phaseSync = new PhaseSync();
    runAsync(() ->
            phaseSync.phase(Phases.SECOND, () ->
                    transactionsWrapper.readUncommitted(() -> accountRepository.deleteById(userAccountId))
                           )
            );

    transactionsWrapper.readCommitted(() -> {
      AtomicInteger existingAmount = new AtomicInteger();
      phaseSync.phase(Phases.FIRST,
          () -> accountRepository.findById(userAccountId).map(Account::getAmount).ifPresent(a -> existingAmount.compareAndSet(0, a)));

      // there is the account with expected amount
      assertThat(existingAmount.get(), is(amount));

      //now another transaction runs in parallel and removes the record

      // there is no such record anymore. but the transaction thinks there is
      phaseSync.phase(Phases.THIRD, () -> accountRepository.updateAmount(userAccountId, amount - fee));

      // the account has not been actually updated, but we're not aware of it
      assertThat(phaseSync.noExceptions(), is(true));

      // and we store the fee that we charged to our system account making the data inconsistent
      accountRepository.updateAmount(systemAccountId, fee);

      // the only way to find out that the account has been removed is to search for it one more time
      // but the application code shouldn't check for data consistency. it's database's responsibility
      assertThat(accountRepository.existsById(userAccountId), is(false));
    });

    phaseSync.phase(Phases.FOURTH, () -> {/*done with all phases*/});
    assertThat(phaseSync.exceptionDetails(), phaseSync.noExceptions(), is(true));
    assertThat(accountRepository.findById(systemAccountId).map(Account::getAmount).orElseThrow(), is(fee));
  }

  @Test
  public void testFixed() {
    //given
    final int userAccountId = 1;
    final int systemAccountId = 2;
    final int amount = 100;
    transactionsWrapper.readCommitted(() -> {
      var account = new Account();
      account.setAmount(amount);
      account.setId(userAccountId);
      accountRepository.saveAndFlush(account);
      var systemAccount = new Account();
      systemAccount.setAmount(0);
      systemAccount.setId(systemAccountId);
      accountRepository.saveAndFlush(systemAccount);
    });

    //expected
    var phaseSync = new PhaseSync();
    runAsync(() ->
            phaseSync.phase(Phases.SECOND, () ->
                    transactionsWrapper.readUncommitted(() -> accountRepository.deleteById(userAccountId))
                           )
            );

    assertThrows(CannotAcquireLockException.class, () -> {
      // the fix is to change readCommitted to repeatableRead
      transactionsWrapper.repeatableReadFallible(() -> {
        AtomicInteger existingAmount = new AtomicInteger();
        phaseSync.phase(Phases.FIRST,
            () -> accountRepository.findById(userAccountId).map(Account::getAmount).ifPresent(a -> existingAmount.compareAndSet(0, a)));
        // there is the account with expected amount
        assertThat(existingAmount.get(), is(amount));

        //now another transaction runs in parallel and removes the record

        // there is no such record anymore. but the transaction thinks there is
        final var fee = 10;
        phaseSync.phase(Phases.THIRD, () -> accountRepository.updateAmount(userAccountId, amount - fee));

        // the account can't be updated because the state we had before we started the transaction is changed by another parallel transaction
        assertThat(phaseSync.noExceptions(), is(false));

        // so, this transaction is failed and will be reverted as soon as the exception we caught is re-thrown.
        assertThat(phaseSync.exceptionDetails(), startsWith("Unexpected exception org.springframework.dao.CannotAcquireLockException"));

        phaseSync.ifAnyExceptionRethrow();

        // the update bringing the system to the inconsistent state is not executed
        accountRepository.updateAmount(systemAccountId, fee);
      });
    });

    phaseSync.phase(Phases.FOURTH, () -> {/*done with all phases*/});
    assertThat(accountRepository.findById(systemAccountId).map(Account::getAmount).orElseThrow(), is(0));
  }

}
