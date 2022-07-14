package me.konoplev.isolation;

import java.lang.annotation.*;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;

@ExtendWith(PostgresTestExtension.class)
@SpringBootTest
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PostgresTest {

}
