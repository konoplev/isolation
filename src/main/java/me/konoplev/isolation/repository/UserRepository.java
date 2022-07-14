package me.konoplev.isolation.repository;

import me.konoplev.isolation.repository.dto.User;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface UserRepository extends JpaRepository<User, Integer> {
  @Modifying(clearAutomatically = true)
  @Query("update User u set u.userName = :newName where u.id = :id")
  void updateUsername(@Param("id") Integer id, @Param("newName") String newName);
}
