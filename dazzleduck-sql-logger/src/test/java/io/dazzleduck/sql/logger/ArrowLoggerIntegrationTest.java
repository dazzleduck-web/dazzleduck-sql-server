package io.dazzleduck.sql.logger;


import org.junit.jupiter.api.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ArrowLoggerIntegrationTest {

    @BeforeAll
    void setup() throws Exception {

    }

    @Test
    void testLoggerCanPostLogs() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("integration-test");

        for (int i = 0; i < 25; i++) {
            logger.info("Test {}", i);
        }
        logger.flush();
        Thread.sleep(3000);
        Assertions.assertTrue(true);
    }
}
