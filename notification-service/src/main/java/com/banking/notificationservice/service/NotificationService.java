package com.banking.notificationservice.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {


    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload
    ) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = payload.get("transactionId").toString();
            String amount = payload.get("amount").toString();
            String reason = payload.get("reason").toString();

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. "+
                                    "Reason: %s "+
                                    "A transaction of %s is pending verification. "+
                                    "Your OTP is: %s . Valid for 5 minutes. "+
                                    "If this wasn't you - ignore this message."
                    )
            );

        } catch (Exception e) {
            log.error("Error: {} sending otp notification",e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {

    }

}
