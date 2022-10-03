package me.konoplev.isolation.repository;

import me.konoplev.isolation.repository.dto.Account;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface AccountRepository extends JpaRepository<Account, Integer> {
  @Modifying(clearAutomatically = true)
  @Query("update Account a set a.amount = :newAmount where a.id = :id")
  void updateAmount(@Param("id") Integer id, @Param("newAmount") int newAmount);

}
