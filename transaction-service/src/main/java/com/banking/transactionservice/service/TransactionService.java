package com.banking.transactionservice.service;


import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dtos.TransactionResponse;
import com.banking.transactionservice.dtos.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.enums.TransactionStatus;
import com.banking.transactionservice.enums.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final RedisTemplate<String,String> redisTemplate;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";


    /*
    * SAGA step 1 - Initiated transfer
    *  Deducted from sender via feign
    *  Saves transaction as PROCESSING
    *  Publish event to kafka for fraud check
    *  Returns.
    * @Param request
    *
    * */
    public TransactionResponse transfer( TransferRequest request) {

        log.info("SAGA START - Transfer: {} -> {} amount: {}",
                request.senderAccountNumber(),
                request.receiverAccountNumber(),
                request.amount());

        //deduct from sender
        accountServiceClient.deductBalance(
                request.receiverAccountNumber(),
                request.amount()
        );

        Transaction transaction = Transaction.builder()
                .senderAccountNumber(request.senderAccountNumber())
                .receiverAccountNumber(request.receiverAccountNumber())
                .amount(request.amount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(request.description())
                .referenceNumber(UUID.randomUUID().toString())
                .build();

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        //STEP -2, CHECK FOR FRAUD DETECTION
        TransactionInitiatedEvent event = new TransactionInitiatedEvent();
        event.setSenderAccountNumber(savedTransaction.getSenderAccountNumber());
        event.setReceiverAccountNumber(savedTransaction.getReceiverAccountNumber());
        event.setAmount(savedTransaction.getAmount());
        event.setDescription(savedTransaction.getDescription());
        event.setTransactionId(savedTransaction.getId());

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(),event);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent published: {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository.findById(transactionId)
                .orElseThrow(()-> new RuntimeException("Transaction Not Found.")));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository.
                findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());

    }

    public TransactionResponse verifyOTP(String transactionId,String otp) {
        log.info("OTP verification for the transaction: {}",transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction Not Found "+transactionId));

        String otpKey = "verification:otp";
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            log.warn("OTP expired for transaction: {}", transactionId);
            transaction = compensateTransaction(transaction,"OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            log.warn("Wrong OTP - blocking account and refunding: {}",transactionId);
            redisTemplate.delete(otpKey);
            blockingAccountAndCompensate(transaction,"Wrong OTP entered - transaction cancelled account blocked for security");
            return mapToResponse(transaction);
        }

        // OTP correct - complete the transaction
        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        Transaction savedTransaction = transactionRepository.save(transaction);
        TransactionCompletedEvent transactionCompletedEvent = new TransactionCompletedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,savedTransaction.getId(),transactionCompletedEvent);
        log.info("SAGA COMPLETED - Transaction {} completed",
                savedTransaction.getId());

    }

    private void blockingAccountAndCompensate(Transaction transaction, String reason) {
        //Publish fraud.detected -> Account Service will block account
        Map<String,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked, kindly contact to the bank",transaction.getSenderAccountNumber());


    }

    private Transaction compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),transaction.getAmount());

        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason+ " SAGA Compensation executed, amount refunded at "+ LocalDateTime.now());
        Transaction savedTransaction = transactionRepository.save(transaction);

        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId",savedTransaction.getId());
        refundEvent.put("senderAccountNumber",savedTransaction.getSenderAccountNumber());
        refundEvent.put("amount",savedTransaction.getAmount());
        refundEvent.put("reason",savedTransaction.getFailureReason());

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,savedTransaction.getId(),refundEvent);
        log.info("SAGA COMPENSATION COMPLETED - {} refunded to {}",
                savedTransaction.getAmount(),savedTransaction.getSenderAccountNumber());
        
        return savedTransaction;

    }

    private TransactionResponse mapToResponse(Transaction transaction) {
        return TransactionResponse.builder()
                .id(transaction.getId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .amount(transaction.getAmount())
                .type(transaction.getType())
                .status(transaction.getStatus())
                .description(transaction.getDescription())
                .failureReason(transaction.getFailureReason())
                .referenceNumber(transaction.getReferenceNumber())
                .createdAt(transaction.getCreatedAt())
                .completedAt(transaction.getCompletedAt())
                .build();
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(()->new RuntimeException("Transaction not found "+transactionId));

        if(transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("Transaction {} not PROCESSING - skipping",transactionId);
            return;
        }
        completeTransaction(transaction);
    }
}
