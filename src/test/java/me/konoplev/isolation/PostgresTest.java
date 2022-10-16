package me.konoplev.isolation;

import java.lang.annotation.*;

import me.konoplev.isolation.PostgresTestExtension.Initializer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@ExtendWith(PostgresTestExtension.class)
@SpringBootTest
@ContextConfiguration(initializers = { Initializer.class})
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PostgresTest {

}
