package me.konoplev.isolation;

import org.junit.jupiter.api.extension.*;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
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
        .withReuse(false);

    container.start();
  }

  public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
          "spring.datasource.url=" + container.getJdbcUrl(),
          "spring.datasource.username=" + container.getUsername(),
          "spring.datasource.password=" + container.getPassword(),
          "spring.jpa.properties.hibernate.dialect=" + "org.hibernate.dialect.PostgreSQLDialect"
                           ).applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    container.stop();
  }
}
