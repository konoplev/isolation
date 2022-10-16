package me.konoplev.isolation;

import org.junit.jupiter.api.extension.*;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static java.util.Objects.nonNull;

@Testcontainers
public class MySqlTestExtension implements BeforeAllCallback, AfterAllCallback {

  private final static String DOCKER_IMAGE = "mysql:8.0.29";
  private final static String DB_USERNAME = "mysql";
  private final static String DB_PASSWORD = "mysql";
  private final static String DB_NAME = "test";
  private final static int DB_PORT = 3306;
  private static MySQLContainer<?> container;

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    container.stop();
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    if (nonNull(container)) {
      return;
    }
    container = new MySQLContainer<>(DOCKER_IMAGE)
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
          "spring.jpa.database-platform=" + "org.hibernate.dialect.MySQL5InnoDBDialect"
                           ).applyTo(configurableApplicationContext.getEnvironment());
    }
  }
}
