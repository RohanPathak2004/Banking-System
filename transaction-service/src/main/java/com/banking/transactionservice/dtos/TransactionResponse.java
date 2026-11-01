package com.banking.transactionservice.dtos;


import com.banking.transactionservice.enums.TransactionStatus;
import com.banking.transactionservice.enums.TransactionType;
import jakarta.persistence.*;
import lombok.Builder;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Builder
public record TransactionResponse(
        String id,
        String senderAccountNumber,
        String receiverAccountNumber,
        BigDecimal amount,
        TransactionType type,
        TransactionStatus status,
        String description,
        String failureReason,
        String referenceNumber,
        LocalDateTime createdAt,
        LocalDateTime completedAt

        ) {
}
