package io.dazzleduck.sql.micrometer.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrowReceiverServer {
    private final HttpServer server;
    private final AtomicInteger receivedCount = new AtomicInteger();
    private final CountDownLatch latch = new CountDownLatch(1);

    public ArrowReceiverServer(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/arrow", new MetricsHandler());
        server.setExecutor(Executors.newCachedThreadPool());
    }

    public void start() {
        server.start();
    }

    public void stop(int delayMillis) {
        server.stop(delayMillis);
    }

    public int receivedCount() {
        return receivedCount.get();
    }

    public boolean awaitFirst(long millis) throws InterruptedException {
        return latch.await(millis, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            // read body, simply count bytes and ack
            byte[] body;
            try (InputStream in = exchange.getRequestBody();
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                byte[] buf = new byte[8192];
                int r;
                while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
                body = baos.toByteArray();
            }
            int now = receivedCount.incrementAndGet();
            System.out.println("ArrowReceiverServer: received payload bytes=" + body.length + " (count=" + now + ")");
            latch.countDown();

            exchange.sendResponseHeaders(200, 0);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write("ok".getBytes());
            }
        }
    }
}
