/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.zaxxer.hikari.pool;

import static com.zaxxer.hikari.pool.TestElf.getPool;
import static com.zaxxer.hikari.pool.TestElf.newHikariConfig;
import static com.zaxxer.hikari.pool.TestElf.setSlf4jLogLevel;
import static com.zaxxer.hikari.pool.TestElf.setSlf4jTargetStream;
import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedMillis;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.mocks.StubConnection;
import com.zaxxer.hikari.mocks.StubDataSource;

public class TestConnectionTimeoutRetry
{
   @Test
   public void testCreationGraceAllowsSlowCreation() throws Exception
   {
      GateDataSource source = new GateDataSource();
      var executor = Executors.newSingleThreadExecutor();
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 0))) {
         try {
            ds.getHikariConfigMXBean().setConnectionCreationWaitTimeout(Long.MAX_VALUE);
            var acquisition = executor.submit(() -> ds.getConnection());
            assertTrue("Creation did not start", source.creationStarted.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> acquisition.get(500, MILLISECONDS));
            source.creationAllowed.countDown();
            try (Connection connection = acquisition.get(2, TimeUnit.SECONDS)) {
               assertNotNull(connection);
               long start = currentTime();
               assertThrows(SQLTransientConnectionException.class, () -> ds.getConnection());
               assertTrue("Saturated pool received creation grace", elapsedMillis(start) < 1500);
            }
         }
         finally {
            source.creationAllowed.countDown();
            executor.shutdownNow();
         }
      }
   }

   @Test
   public void testSpareCapacityWithoutCreationDoesNotReceiveGrace() throws Exception
   {
      GateDataSource source = new GateDataSource();
      CountDownLatch adderStarted = new CountDownLatch(1);
      CountDownLatch adderAllowed = new CountDownLatch(1);
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 5000))) {
         var field = HikariPool.class.getDeclaredField("addConnectionExecutor");
         field.setAccessible(true);
         var adder = (ThreadPoolExecutor) field.get(getPool(ds));
         var blocker = adder.submit(() -> {
            adderStarted.countDown();
            return adderAllowed.await(10, TimeUnit.SECONDS);
         });
         try {
            assertTrue("Adder did not start", adderStarted.await(2, TimeUnit.SECONDS));
            long start = currentTime();
            assertThrows(SQLTransientConnectionException.class, () -> ds.getConnection());
            assertTrue("Queued creation received grace", elapsedMillis(start) < 1500);
            assertEquals("No physical creation should have started", 1L, source.creationStarted.getCount());
         }
         finally {
            adderAllowed.countDown();
            source.creationAllowed.countDown();
            assertTrue("Adder was not released", blocker.get(2, TimeUnit.SECONDS));
         }
      }
   }

   @Test
   public void testCreationGraceDisabledByDefault() throws Exception
   {
      GateDataSource source = new GateDataSource();
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 0))) {
         try {
            long start = currentTime();
            assertThrows(SQLTransientConnectionException.class, () -> ds.getConnection());
            assertTrue("Acquisition returned before its deadline", elapsedMillis(start) >= 250);
            assertTrue("Default timeout was extended", elapsedMillis(start) < 1500);
         }
         finally {
            source.creationAllowed.countDown();
         }
      }
   }

   @Test
   public void testCreationGraceDoesNotExtendExplicitTimeout() throws Exception
   {
      GateDataSource source = new GateDataSource();
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 5000))) {
         try {
            long start = currentTime();
            assertThrows(SQLTransientConnectionException.class, () -> getPool(ds).getConnection(250));
            assertTrue("Explicit deadline was extended", elapsedMillis(start) < 1500);
         }
         finally {
            source.creationAllowed.countDown();
         }
      }
   }

   @Test
   public void testCreationGraceIsBoundedAcrossFailedRetries() throws Exception
   {
      GateDataSource source = new GateDataSource();
      source.creationFailure = new SQLException("Connection refused");
      var executor = Executors.newSingleThreadExecutor();
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 1000))) {
         try {
            long start = currentTime();
            var acquisition = executor.submit(() -> ds.getConnection());
            assertTrue("Creation did not start", source.creationStarted.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> acquisition.get(500, MILLISECONDS));
            source.creationAllowed.countDown();
            var failure = assertThrows(ExecutionException.class, () -> acquisition.get(2, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof SQLTransientConnectionException);
            assertTrue("Acquisition returned before the combined deadline", elapsedMillis(start) >= 1250);
            assertTrue("Retries renewed the grace period", elapsedMillis(start) < 2500);
         }
         finally {
            source.creationAllowed.countDown();
            executor.shutdownNow();
         }
      }
   }

   @Test
   public void testCreationGraceIsBoundedForBlockedCreation() throws Exception
   {
      GateDataSource source = new GateDataSource();
      var executor = Executors.newSingleThreadExecutor();
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 1000))) {
         try {
            long start = currentTime();
            var acquisition = executor.submit(() -> ds.getConnection());
            assertTrue("Creation did not start", source.creationStarted.await(2, TimeUnit.SECONDS));
            var failure = assertThrows(ExecutionException.class, () -> acquisition.get(2500, MILLISECONDS));
            assertTrue(failure.getCause() instanceof SQLTransientConnectionException);
            assertTrue("Creation did not receive grace", elapsedMillis(start) >= 1250);
            assertTrue("Creation wait exceeded its combined budget", elapsedMillis(start) < 2500);
         }
         finally {
            source.creationAllowed.countDown();
            executor.shutdownNow();
         }
      }
   }

   @Test
   public void testCreationGraceCanBeInterrupted() throws Exception
   {
      GateDataSource source = new GateDataSource();
      var executor = Executors.newSingleThreadExecutor();
      try (HikariDataSource ds = new HikariDataSource(creationWaitConfig(source, 5000))) {
         try {
            var acquisition = executor.submit(() -> {
               try (Connection connection = ds.getConnection()) {
                  return false;
               }
               catch (SQLException e) {
                  return Thread.currentThread().isInterrupted() && e.getMessage().contains("Interrupted during connection acquisition");
               }
            });
            assertTrue("Creation did not start", source.creationStarted.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> acquisition.get(500, MILLISECONDS));
            executor.shutdownNow();
            assertTrue("Acquisition did not preserve interruption", acquisition.get(1, TimeUnit.SECONDS));
         }
         finally {
            source.creationAllowed.countDown();
            executor.shutdownNow();
         }
      }
   }

   @Test
   public void testCreationGraceDoesNotRequireNewConnection() throws Exception
   {
      GateDataSource source = new GateDataSource();
      var executor = Executors.newSingleThreadExecutor();
      HikariConfig config = creationWaitConfig(source, 2000);
      config.setMaximumPoolSize(2);
      try (HikariDataSource ds = new HikariDataSource(config)) {
         source.creationAllowed.countDown();
         Connection first = ds.getConnection();
         try {
            source.creationAllowed = new CountDownLatch(1);
            source.creationStarted = new CountDownLatch(1);
            var acquisition = executor.submit(() -> ds.getConnection());
            assertTrue("Creation did not start", source.creationStarted.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> acquisition.get(500, MILLISECONDS));
            first.close();
            try (Connection returned = acquisition.get(1, TimeUnit.SECONDS)) {
               assertNotNull(returned);
               assertEquals("A new connection should still be blocked", 1, getPool(ds).getTotalConnections());
            }
         }
         finally {
            first.close();
            source.creationAllowed.countDown();
            executor.shutdownNow();
         }
      }
   }

   private HikariConfig creationWaitConfig(GateDataSource source, long creationWait)
   {
      HikariConfig config = newHikariConfig();
      config.setDataSource(source);
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(1);
      config.setInitializationFailTimeout(-1);
      config.setConnectionTimeout(250);
      if (creationWait > 0) {
         config.setConnectionCreationWaitTimeout(creationWait);
      }
      return config;
   }

   private static class GateDataSource extends StubDataSource
   {
      volatile CountDownLatch creationStarted = new CountDownLatch(1);
      volatile CountDownLatch creationAllowed = new CountDownLatch(1);
      volatile SQLException creationFailure;

      @Override
      public Connection getConnection() throws SQLException
      {
         creationStarted.countDown();
         try {
            if (!creationAllowed.await(10, TimeUnit.SECONDS)) {
               throw new SQLException("Test did not release connection creation");
            }
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Connection creation interrupted", e);
         }
         if (creationFailure != null) {
            throw creationFailure;
         }
         return super.getConnection();
      }
   }

   @Test
   public void testConnectionRetries() throws SQLException
   {
      HikariConfig config = newHikariConfig();
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(1);
      config.setConnectionTimeout(2800);
      config.setValidationTimeout(2800);
      config.setConnectionTestQuery("VALUES 1");
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

      try (HikariDataSource ds = new HikariDataSource(config)) {
         StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
         stubDataSource.setThrowException(new SQLException("Connection refused"));

         long start = currentTime();
         try (Connection connection = ds.getConnection()) {
            connection.close();
            fail("Should not have been able to get a connection.");
         }
         catch (SQLException e) {
            long elapsed = elapsedMillis(start);
            long timeout = config.getConnectionTimeout();
            assertTrue("Didn't wait long enough for timeout", (elapsed >= timeout));
         }
      }
   }

   @Test
   public void testConnectionRetries2() throws SQLException
   {
      HikariConfig config = newHikariConfig();
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(1);
      config.setConnectionTimeout(2800);
      config.setValidationTimeout(2800);
      config.setInitializationFailTimeout(0);
      config.setConnectionTestQuery("VALUES 1");
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

      try (HikariDataSource ds = new HikariDataSource(config)) {
         final StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
         stubDataSource.setThrowException(new SQLException("Connection refused"));

         ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
         scheduler.schedule(new Runnable() {
            @Override
            public void run()
            {
               stubDataSource.setThrowException(null);
            }
         }, 300, TimeUnit.MILLISECONDS);

         long start = currentTime();
         try {
            try (Connection connection = ds.getConnection()) {
               // close immediately
            }

            long elapsed = elapsedMillis(start);
            assertTrue("Connection returned too quickly, something is wrong.", elapsed > 250);
            assertTrue("Waited too long to get a connection.", elapsed < config.getConnectionTimeout());
         }
         catch (SQLException e) {
            fail("Should not have timed out: " + e.getMessage());
         }
         finally {
            scheduler.shutdownNow();
         }
      }
   }

   @Test
   public void testConnectionRetries3() throws SQLException
   {
      HikariConfig config = newHikariConfig();
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(2);
      config.setConnectionTimeout(2800);
      config.setValidationTimeout(2800);
      config.setConnectionTestQuery("VALUES 1");
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

      try (HikariDataSource ds = new HikariDataSource(config)) {
         final Connection connection1 = ds.getConnection();
         final Connection connection2 = ds.getConnection();
         assertNotNull(connection1);
         assertNotNull(connection2);

         ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
         scheduler.schedule(new Runnable() {
            @Override
            public void run()
            {
               try {
                  connection1.close();
               }
               catch (Exception e) {
                  e.printStackTrace(System.err);
               }
            }
         }, 800, MILLISECONDS);

         long start = currentTime();
         try {
            try (Connection connection3 = ds.getConnection()) {
               // close immediately
            }

            long elapsed = elapsedMillis(start);
            assertTrue("Waited too long to get a connection.", (elapsed >= 700) && (elapsed < 950));
         }
         catch (SQLException e) {
            fail("Should not have timed out.");
         }
         finally {
            scheduler.shutdownNow();
         }
      }
   }

   @Test
   public void testConnectionRetries5() throws SQLException
   {
      HikariConfig config = newHikariConfig();
      config.setMinimumIdle(0);
      config.setMaximumPoolSize(2);
      config.setConnectionTimeout(1000);
      config.setValidationTimeout(1000);
      config.setConnectionTestQuery("VALUES 1");
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

      try (HikariDataSource ds = new HikariDataSource(config)) {
         final Connection connection1 = ds.getConnection();

         long start = currentTime();

         ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
         scheduler.schedule(new Runnable() {
            @Override
            public void run()
            {
               try {
                  connection1.close();
               }
               catch (Exception e) {
                  e.printStackTrace(System.err);
               }
            }
         }, 250, MILLISECONDS);

         StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
         stubDataSource.setThrowException(new SQLException("Connection refused"));

         try {
            try (Connection connection2 = ds.getConnection()) {
               // close immediately
            }

            long elapsed = elapsedMillis(start);
            assertTrue("Waited too long to get a connection.", (elapsed >= 250) && (elapsed < config.getConnectionTimeout()));
         }
         catch (SQLException e) {
            fail("Should not have timed out.");
         }
         finally {
            scheduler.shutdownNow();
         }
      }
   }

   @Test
   public void testConnectionIdleFill() throws Exception
   {
      StubConnection.slowCreate = false;

      HikariConfig config = newHikariConfig();
      config.setMinimumIdle(5);
      config.setMaximumPoolSize(10);
      config.setConnectionTimeout(2000);
      config.setValidationTimeout(2000);
      config.setConnectionTestQuery("VALUES 2");
      config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

      System.setProperty("com.zaxxer.hikari.housekeeping.periodMs", "400");

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos, true);
      setSlf4jTargetStream(HikariPool.class, ps);

      try (HikariDataSource ds = new HikariDataSource(config)) {
         setSlf4jLogLevel(HikariPool.class, Level.DEBUG);

         HikariPool pool = getPool(ds);
         try (
            Connection connection1 = ds.getConnection();
            Connection connection2 = ds.getConnection();
            Connection connection3 = ds.getConnection();
            Connection connection4 = ds.getConnection();
            Connection connection5 = ds.getConnection();
            Connection connection6 = ds.getConnection();
            Connection connection7 = ds.getConnection()) {

            sleep(1300);

            assertSame("Total connections not as expected", 10, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 3, pool.getIdleConnections());
         }

         assertSame("Total connections not as expected", 10, pool.getTotalConnections());
         assertSame("Idle connections not as expected", 10, pool.getIdleConnections());
      }
   }

   @Before
   public void before()
   {
      setSlf4jLogLevel(HikariPool.class, Level.INFO);
   }

   @After
   public void after()
   {
      System.getProperties().remove("com.zaxxer.hikari.housekeeping.periodMs");
      setSlf4jLogLevel(HikariPool.class, Level.INFO);
   }
}
