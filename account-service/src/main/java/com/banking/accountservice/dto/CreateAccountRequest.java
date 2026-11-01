package com.banking.accountservice.dto;


import com.banking.accountservice.entity.AccountType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;


import java.math.BigDecimal;


public record CreateAccountRequest(
        @NotBlank(message = "Account Holder name cannot be blank")
        String accountHolderName,

        @NotBlank(message = "Email cannot be blank")
        String email,

        @NotBlank(message = "Phone number cannot be blank")
        String phone,

        @NotNull(message = "Account type is required")
        AccountType accountType,

        @NotNull(message = "Initial deposit required")
        @Positive(message = "Initial deposit must be positive")
        BigDecimal initialDeposit
) {

}
