package com.banking.accountservice.service;


import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    private BigDecimal getTransactionLimitByAccountType(AccountType accountType) {
        return switch (accountType) {
            case SAVINGS -> new BigDecimal("100_000");
            case CURRENT -> new BigDecimal("500_000");
            case FIXED_DEPOSIT -> new BigDecimal("10_000");
        };
    }

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account for: {}", request.email());
        if (accountRepository.existsByEmail(request.email())) {
            throw new RuntimeException("Account already exists " + request.email());
        }

        Account account = Account.builder()
                .accountHolderName(request.accountHolderName())
                .email(request.email())
                .phone(request.phone())
                .accountType(request.accountType())
                .status(AccountStatus.ACTIVE)
                .balance(request.initialDeposit())
                .accountNumber(generateAccountNumber())
                .dailyTransactionLimit(getTransactionLimitByAccountType(request.accountType()))
                .build();

        Account savedAccount = accountRepository.save(account);
        log.info("Account created : {}", savedAccount.getAccountNumber());

        return mapToResponse(savedAccount);
    }

    //generate unique 12 digits account number
    private String generateAccountNumber() {
        String accountNumber;
        do {
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        } while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account) {
        AccountResponse response = AccountResponse.builder()
                .id(account.getId())
                .accountHolderName(account.getAccountHolderName())
                .accountNumber(account.getAccountNumber())
                .accountType(account.getAccountType())
                .email(account.getEmail())
                .phone(account.getPhone())
                .status(account.getStatus())
                .balance(account.getBalance())
                .dailyTransactionLimit(account.getDailyTransactionLimit())
                .createdAt(account.getCreatedAt())
                .build();
        return response;
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        return mapToResponse(account);
    }

    /*
     * Bank Account - get account balance
     * */
    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        return account.getBalance();
    }


    /*
     * Block account - called by FRAUD DETECTION SERVICE VIA KAFKA
     * */
    public void blockAccount(String accountNumber) {
        log.info("Blocking account :{}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account Blocked account: {}", account);
    }

    /*
     * deduct amount by sender account
     * */
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("deducting balance from account: {} of balance: {}", accountNumber, amount);
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        if(account.getStatus() != AccountStatus.ACTIVE)
            throw new RuntimeException("Account is not active");

        if (account.getBalance().compareTo(amount) < 0)
            throw new RuntimeException("Not enough balance for transaction");


        BigDecimal newBalance = (account.getBalance()).subtract(new BigDecimal(String.valueOf(amount)));
        account.setBalance(newBalance);
        Account savedAccount = accountRepository.save(account);
        log.info("Amount of: {} is deducted from account number: {}, new balance: {}",amount,accountNumber,savedAccount.getBalance());
    }

    /*
    * Credit balance
    * Called by Transaction Service via kafka
    * @param accountNumber
    * @param amount
    * */
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting: {} to account: {}",amount,accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not found"));
        account.setBalance(account.getBalance().add(amount));
        Account savedAccount = accountRepository.save(account);
        log.info("Credited: {} in account: {}, updated balance: {}",amount,accountNumber,savedAccount.getBalance());
    }
}
