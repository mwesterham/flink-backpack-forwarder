package me.matthew.flink.backpacktfforward.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BackpackTfApiClient.
 * Tests basic functionality without requiring actual API calls.
 */
class BackpackTfApiClientTest {

    @Test
    void testConstructor_WithValidToken_CreatesClient() {
        // Test that the client can be created with a valid token
        String validToken = "test-token-123";
        
        assertDoesNotThrow(() -> {
            BackpackTfApiClient client = new BackpackTfApiClient(validToken);
            assertNotNull(client);
        });
    }

    @Test
    void testConstructor_WithNullToken_ThrowsException() {
        // Test that null token throws exception
        assertThrows(IllegalArgumentException.class, () -> {
            new BackpackTfApiClient(null);
        });
    }

    @Test
    void testConstructor_WithEmptyToken_ThrowsException() {
        // Test that empty token throws exception
        assertThrows(IllegalArgumentException.class, () -> {
            new BackpackTfApiClient("");
        });
    }

    @Test
    void testConstructor_WithWhitespaceToken_ThrowsException() {
        // Test that whitespace-only token throws exception
        assertThrows(IllegalArgumentException.class, () -> {
            new BackpackTfApiClient("   ");
        });
    }

    @Test
    void testDefaultConstructor_WithoutEnvironmentVariable_ThrowsException() {
        // Test that default constructor throws exception when env var is not set
        // This will fail if BACKPACK_TF_API_TOKEN is actually set in the environment
        assertThrows(IllegalStateException.class, () -> {
            new BackpackTfApiClient();
        });
    }
}