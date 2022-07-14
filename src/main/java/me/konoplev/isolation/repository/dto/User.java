package me.konoplev.isolation.repository.dto;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Entity
@Table(name = "users")
@Data
public class User {

  @Id
  @GeneratedValue
  private Integer id;

  @Column(unique=true, name = "user_name")
  private String userName;

  @OneToMany(fetch = FetchType.EAGER, mappedBy = "user", cascade = CascadeType.ALL)
  private List<Account> accounts = new ArrayList<>();

}
