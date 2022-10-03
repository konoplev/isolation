package me.konoplev.isolation;

import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static java.util.Objects.nonNull;

@Testcontainers
public class PostgresTestExtension implements BeforeAllCallback, AfterAllCallback {
  private final static String DOCKER_IMAGE = "postgres:13.3-alpine";
  private final static String DB_USERNAME = "postgres";
  private final static String DB_PASSWORD = "postgres";
  private final static String DB_NAME = "test";
  private final static int DB_PORT = 5432;

  private static PostgreSQLContainer<?> container;

  @Override
  public void beforeAll(ExtensionContext context) {
    if (nonNull(container)) return;

    container = new PostgreSQLContainer<>(DOCKER_IMAGE)
        .withDatabaseName(DB_NAME)
        .withUsername(DB_USERNAME)
        .withPassword(DB_PASSWORD)
        .withExposedPorts(DB_PORT)
        .withReuse(true);

    container.start();
    System.setProperty("spring.datasource.url", container.getJdbcUrl());
    System.setProperty("spring.datasource.username", container.getUsername());
    System.setProperty("spring.datasource.password", container.getPassword());
    System.setProperty("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    container.stop();
  }
}
