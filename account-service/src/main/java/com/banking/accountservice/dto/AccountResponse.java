package com.banking.accountservice.dto;

import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Builder
public record AccountResponse(

        String id,

        String accountNumber,

        String accountHolderName,

        String email,

        String phone,


        AccountType accountType,


        AccountStatus status,

        BigDecimal balance,

        BigDecimal dailyTransactionLimit,


        LocalDateTime createdAt,

        LocalDateTime updatedAt
        ) {
}
