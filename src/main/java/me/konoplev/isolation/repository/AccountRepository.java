package me.konoplev.isolation.repository;

import me.konoplev.isolation.repository.dto.Account;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountRepository extends JpaRepository<Account, Integer> {

}
