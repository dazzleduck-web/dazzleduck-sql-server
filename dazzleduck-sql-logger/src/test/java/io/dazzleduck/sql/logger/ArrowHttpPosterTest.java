package io.dazzleduck.sql.logger;

import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ArrowHttpPosterTest {

    private ArrowHttpPoster poster;
    private HttpClient mockClient;

    // ---- Custom subclass to disable background threads ----
    static class TestArrowHttpPoster extends ArrowHttpPoster {
        public TestArrowHttpPoster(String url, int q, int b, Duration d) {
            super(url, q, b, d);
        }

        @Override
        protected ScheduledExecutorService createScheduler() {
            // Prevents drainLoop + flush threads from running
            return mock(ScheduledExecutorService.class);
        }
    }

    @BeforeEach
    void setup() throws Exception {
        poster = new TestArrowHttpPoster("http://localhost:9999/test", 100, 1, Duration.ofMillis(200));

        mockClient = mock(HttpClient.class);
        var field = ArrowHttpPoster.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(poster, mockClient);

        HttpResponse<String> ok = mock(HttpResponse.class);
        when(ok.statusCode()).thenReturn(200);
        when(mockClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(ok);
    }

    @AfterEach
    void teardown() {
        poster.close();
    }

    // ------------------------------------------------------------
    @Test
    void testEnqueueSuccess() {
        assertTrue(poster.enqueue("hello".getBytes()));
    }

    // ------------------------------------------------------------
    @Test
    void testFlushSendsHttpPost() throws Exception {
        byte[] batch1 = "A1".getBytes();
        byte[] batch2 = "A2".getBytes();

        poster.enqueue(batch1);
        poster.enqueue(batch2);

        Method sendAll = ArrowHttpPoster.class.getDeclaredMethod("sendAll", List.class);
        sendAll.setAccessible(true);
        sendAll.invoke(poster, List.of(batch1, batch2));

        verify(mockClient, times(2))
                .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    void testDrainLoopManualSendAll() throws Exception {
        byte[] payload = "batch".getBytes();
        poster.enqueue(payload);

        Method sendAll = ArrowHttpPoster.class.getDeclaredMethod("sendAll", List.class);
        sendAll.setAccessible(true);
        sendAll.invoke(poster, List.of(payload));

        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);

        verify(mockClient, times(1))
                .send(captor.capture(), any(HttpResponse.BodyHandler.class));

        assertEquals(URI.create("http://localhost:9999/test"), captor.getValue().uri());
    }

    // ------------------------------------------------------------
    @Test
    void testHttpFailureThrowsException() throws Exception {
        HttpResponse<String> badResp = mock(HttpResponse.class);
        when(badResp.statusCode()).thenReturn(500);
        when(badResp.body()).thenReturn("failed");
        when(mockClient.send(any(), any(HttpResponse.BodyHandler.class))).thenReturn(badResp);

        Method sendPost = ArrowHttpPoster.class.getDeclaredMethod("sendHttpPost", byte[].class);
        sendPost.setAccessible(true);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            try {
                sendPost.invoke(poster, "bad".getBytes());
            } catch (Exception e) {
                throw (RuntimeException) e.getCause(); // unwrap
            }
        });

        assertTrue(ex.getMessage().contains("HTTP POST failed"));
    }

    @Test
    void testCloseStopsRunning() throws Exception {
        poster.close();

        var field = ArrowHttpPoster.class.getDeclaredField("running");
        field.setAccessible(true);
        assertFalse(field.getBoolean(poster));
    }
}
