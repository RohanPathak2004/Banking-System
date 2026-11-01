package com.banking.transactionservice.dtos;

import com.banking.transactionservice.enums.TransactionType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.math.BigDecimal;

@Builder
public record TransferRequest(
        @NotBlank(message = "Sender account number is required.")
        String senderAccountNumber,
        @NotBlank(message = "Receiver account number is required.")
        String receiverAccountNumber,
        @NotNull(message = "Amount is required")
        @Positive(message = "Amount must be positive")
        BigDecimal amount,
        String description
) {
}
