package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

//listen event for example from transaction
@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {



    private final AccountService accountService;

    /*
    * Consumer transaction.completed event from kafka
    * Credits receiver account
    *
    * */
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
    ){

        try{
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} amount: {}", receiverAccount,amount);
            accountService.creditBalance(receiverAccount,amount);

        } catch (Exception e) {
            log.error("Error crediting account: {}",e.getMessage());
        }

    }

}
