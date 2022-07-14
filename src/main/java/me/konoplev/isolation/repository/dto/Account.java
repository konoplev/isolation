package me.konoplev.isolation.repository.dto;

import javax.persistence.*;

import lombok.Data;
import lombok.ToString;

@Entity
@Table(name = "account")
@Data
public class Account {

  @Id
  @GeneratedValue
  private Integer id;

  @ManyToOne
  @JoinColumn(name = "user_id")
  @ToString.Exclude
  private User user;

  private int amount;

}
