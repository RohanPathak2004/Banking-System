package com.banking.transactionservice.service;


import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.enums.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@KafkaListener()
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {


    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;

    private final RedisTemplate<String,String> redisTemplate;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final long OTP_EXPIRY_MINUTES = 5;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String TRANSACTION_OTP_GENERATED_OTP = "transaction.otp.generated.otp";

    /*
    * Consume verificatin.required
    * Generate OTP and ask user to verify
    * @param Payload
    * */
    @KafkaListener(topics = "verification.required")
    public void consumerVerificationRequired(
            @Payload Map<String, Object> payload
    ) {

        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");
            log.info("Verification require - transaction: {} reason: {}",
                    transactionId,reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(()-> new RuntimeException("Transaction Not Found "+ transactionId));

            if(transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.warn("Transaction {} not processing - skipping ", transactionId);
            }

            String otp = String.format("%06d",RANDOM.nextInt(1_000_000));

            //Store otp in redis - expires in 5 min
            String otpKey = "verification:otp"+transactionId;
            redisTemplate.opsForValue().set(otpKey,otp,OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);


            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            Transaction pendingTransaction = transactionRepository.save(transaction);
            log.info("OTP generated for transaction: {} expires in {} min",
                    transactionId,
                    OTP_EXPIRY_MINUTES);

            Map<String,Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId",transactionId);
            otpEvent.put("accountNumber",accountNumber);
            otpEvent.put("reason",reason);
            otpEvent.put("otp",otp);
            otpEvent.put("amount",payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_OTP,transactionId,otpEvent);


        } catch (Exception e) {
            log.error("Error handling verification required: {}  ",e.getMessage());
        }


    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(
            @Payload Map<String,Object> payload
    ) {

        try{
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);
        } catch (Exception e) {
            log.error("Error processing fraud check result: {}",e.getMessage());
        }

    }

}
