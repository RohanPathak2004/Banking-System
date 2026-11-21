package com.banking.notificationservice.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
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
                            "Suspicious activity detected on your account. " +
                                    "Reason: %s " +
                                    "A transaction of %s is pending verification. " +
                                    "Your OTP is: %s . Valid for 5 minutes. " +
                                    "If this wasn't you - ignore this message."
                    )
            );

        } catch (Exception e) {
            log.error("Error: {} sending otp notification", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            // debit alert to sender
            String debitMessage = String.format(
                    "%s debited from account %s",
                    amount, senderAccount
            );
            sendAlert(senderAccount, "DEBIT ALERT", debitMessage);

            // credit alert to receiver
            String creditMessage = String.format(
                    "%s credit form account %s",
                    amount, receiverAccount
            );
            sendAlert(receiverAccount, "CREDIT ALERT", creditMessage);

        } catch (Exception e) {
            log.error("Error in consuming transaction completed notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload) {

        try {
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            // fraud alert
            String message = String.format(
                    "Your account %s has been locked. " +
                            "reason - %s. Contact your bank immediately.",
                    reason
            );
            sendAlert(accountNumber, "SUSPICIOUS ACTIVITY DETECTED", message);

        } catch (Exception e) {
            log.error("Error in consume Fraud Detection Alert: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String, Object> payload) {

        try {
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");

            String message = String.format(
                    "amount %s is refunded to your account %s reason - %s",
                    amount, senderAccountNumber, reason
            );

            // refund alert
            sendAlert(senderAccountNumber, "REFUND PROCESSED", message);

        } catch (Exception e) {
            log.error("Error sending transaction refund - {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");
            String razorpayPaymentId = (String) payload.get("razorpayPaymentId");

            // payment successful alert
            String message = String.format(
                    "Payment of %s was successful. Razorpay ID: %s",
                    amount, razorpayPaymentId
            );
            sendAlert(accountNumber, "PAYMENT SUCCESSFUL", message);

        } catch (Exception e) {
            log.error("Error in payment completed alert - {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String,Object> payload) {

        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");

            // payment failed alert
            String message = String.format(
                    "Payment of %s was failed and please try again or contact support." ,
                    amount
            );
            sendAlert(accountNumber, "PAYMENT SUCCESSFUL", message);

        } catch (Exception e) {
            log.error("Error sending payment failed alert - {}",e.getMessage());
        }

    }

    private void sendAlert(String accountNumber, String subject, String message) {

        log.info("-----------------------------------------");
        log.info("Account: {}",accountNumber);
        log.info("Subject: {}",subject);
        log.info("Message: {}",message);
        log.info("-----------------------------------------");
    }

}
