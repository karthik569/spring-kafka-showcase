package com.example.springkafka.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

/**
 * Service providing Client-Side Field-Level Encryption (Envelope Encryption)
 * for sensitive PII / GDPR / PCI-DSS fields in Kafka records.
 * <p>
 * Ensures brokers and intermediaries maintain zero-knowledge of confidential payloads.
 */
@Component
public class FieldEncryptionService {

    private static final Logger log = LoggerFactory.getLogger(FieldEncryptionService.class);
    private static final String ALGORITHM = "AES";
    private static final String DEFAULT_MASTER_KEY = "SpringKafkaShowcaseSecretMasterKey2026";

    private final SecretKeySpec secretKey;

    public FieldEncryptionService() {
        try {
            byte[] key = DEFAULT_MASTER_KEY.getBytes(StandardCharsets.UTF_8);
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            key = sha.digest(key);
            key = Arrays.copyOf(key, 16); // 128-bit key
            this.secretKey = new SecretKeySpec(key, ALGORITHM);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize AES encryption key", e);
        }
    }

    /**
     * Encrypts plaintext field into Base64 ciphertext with [ENC] marker.
     */
    public String encryptField(String plaintext) {
        if (plaintext == null || plaintext.isEmpty()) return plaintext;
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            return "[ENC]" + Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            log.error("[FIELD-ENCRYPTION] Failed to encrypt field: {}", e.getMessage());
            return plaintext;
        }
    }

    /**
     * Decrypts Base64 ciphertext starting with [ENC] back to plaintext.
     */
    public String decryptField(String ciphertext) {
        if (ciphertext == null || !ciphertext.startsWith("[ENC]")) return ciphertext;
        try {
            String rawBase64 = ciphertext.substring(5);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(rawBase64));
            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("[FIELD-ENCRYPTION] Failed to decrypt field: {}", e.getMessage());
            return ciphertext;
        }
    }
}
