package me.konoplev.isolation;

import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static java.util.Objects.nonNull;

@Testcontainers
public class MySqlTestExtension implements BeforeAllCallback, AfterAllCallback {

  private final static String DOCKER_IMAGE = "mysql:8.0.29-oracle";
  private final static String DB_USERNAME = "postgres";
  private final static String DB_PASSWORD = "postgres";
  private final static String DB_NAME = "test";
  private final static int DB_PORT = 5432;
  private static MySQLContainer<?> container;

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    container.stop();
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    if (nonNull(container)) {
      return;
    }

    container = new MySQLContainer<>("mysql:8.0.29");
    container.start();
    System.setProperty("spring.datasource.url", container.getJdbcUrl());
    System.setProperty("spring.datasource.username", container.getUsername());
    System.setProperty("spring.datasource.password", container.getPassword());
    System.setProperty("spring.jpa.database-platform", "org.hibernate.dialect.MySQL5InnoDBDialect");
  }
}
