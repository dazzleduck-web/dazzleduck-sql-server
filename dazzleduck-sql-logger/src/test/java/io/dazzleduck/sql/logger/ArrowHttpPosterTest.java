package io.dazzleduck.sql.logger;

import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ArrowHttpPosterTest {

    private ArrowHttpPoster poster;
    private HttpClient mockClient;

    @BeforeEach
    void setup() throws Exception {
        poster = new ArrowHttpPoster("http://localhost:9999/test", 100, 1, Duration.ofMillis(200));

        mockClient = mock(HttpClient.class);
        Field f = ArrowHttpPoster.class.getDeclaredField("httpClient");
        f.setAccessible(true);
        f.set(poster, mockClient);

        HttpResponse<String> okResponse = mock(HttpResponse.class);
        when(okResponse.statusCode()).thenReturn(200);
        when(mockClient.send(any(), any(HttpResponse.BodyHandler.class)))
                .thenReturn(okResponse);
    }

    @AfterEach
    void teardown() {
        poster.close();
    }

    // ------------------------------------------------------------
    @Test
    void testEnqueueSuccess() {
        byte[] data = "hello".getBytes();
        assertTrue(poster.enqueue(data), "Enqueue should succeed when running");
    }

    // ------------------------------------------------------------
    @Test
    void testFlushSendsHttpPost() throws Exception {
        byte[] batch1 = "A1".getBytes();
        byte[] batch2 = "A2".getBytes();

        poster.enqueue(batch1);
        poster.enqueue(batch2);

        // Trigger flush manually via reflection
        var flushMethod = ArrowHttpPoster.class.getDeclaredMethod("flushSafely");
        flushMethod.setAccessible(true);
        flushMethod.invoke(poster);

        // Verify HTTP POST calls
        verify(mockClient, times(2))
                .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    // ------------------------------------------------------------
    @Test
    void testDrainLoopSendsAll() throws Exception {
        byte[] payload = "batch".getBytes();
        poster.enqueue(payload);

        var sendMethod = ArrowHttpPoster.class.getDeclaredMethod("sendAll", List.class);
        sendMethod.setAccessible(true);
        sendMethod.invoke(poster, List.of(payload));

        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);

        verify(mockClient, times(1))
                .send(captor.capture(), any(HttpResponse.BodyHandler.class));

        HttpRequest req = captor.getValue();
        assertEquals(URI.create("http://localhost:9999/test"), req.uri());
    }

    // ------------------------------------------------------------
    @Test
    void testHttpFailureThrowsException() throws Exception {
        HttpResponse<String> failResp = mock(HttpResponse.class);
        when(failResp.statusCode()).thenReturn(500);
        when(failResp.body()).thenReturn("failed");
        when(mockClient.send(any(), any(HttpResponse.BodyHandler.class)))
                .thenReturn(failResp);

        var sendMethod = ArrowHttpPoster.class.getDeclaredMethod("sendHttpPost", byte[].class);
        sendMethod.setAccessible(true);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> sendMethod.invoke(poster, "bad".getBytes()));

        assertTrue(ex.getMessage().contains("HTTP POST failed"));
    }

    @Test
    void testCloseStopsRunning() throws Exception {
        poster.close();

        Field runningField = ArrowHttpPoster.class.getDeclaredField("running");
        runningField.setAccessible(true);

        boolean running = runningField.getBoolean(poster);
        assertFalse(running, "Poster must stop running after close()");
    }
}
