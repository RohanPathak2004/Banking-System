package com.banking.paymentservice.service;


import com.banking.paymentservice.dtos.CreatePaymentRequest;
import com.banking.paymentservice.dtos.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.enums.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    private final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private final String PAYMENT_FAILED_TOPIC = "payment.failed";


    /*
    * Create Razorpay payment order.
    *
    * FLOW:
    *  1. Create order in razorpay
    *  2. Save payment record in db
    *  3. return order details in frontend
    *  4. frontend shows Razorpay checkout
    *  5. User pays
    *  6. Razorpay calls webhook
    *
    * */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order for account: {} amount: {}",
                request.getAccountNumber(),
                request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount",convertedAmount);
        orderRequest.put("currency","USD/INR");
        String uniqueReceiptId = "rcpt_"+ System.currentTimeMillis()+UUID.randomUUID().toString().replace("-","").substring(0,10);
        orderRequest.put("receipt",uniqueReceiptId);

        Order razorPayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}", razorPayOrder.get("id").toString());

        Payment payment = new Payment();
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency(razorPayOrder.get("USD/INR"));
        payment.setRazorpayOrderId(razorPayOrder.get("id"));
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());


        Payment savedPayment = paymentRepository.save(payment);
        return mapToResponse(savedPayment);
    }

    public void handleWebHook(Map<String,Object> payload){
        log.info("Received Razorpay webhook: {}",payload.get("event"));
        String event  = (String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }else if("payment.failed".equals(event)){
            handlePaymentFail(payload);
        }
    }

    private void  handlePaymentFail(Map<String, Object> payload) {
        try {
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()->new RuntimeException("Payment not found for order id - "+orderId));
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            Payment savedPayment = paymentRepository.save(payment);
            Map<String,Object> event = new HashMap<>();
            event.put("paymentId",savedPayment.getId());
            event.put("accountNumber",savedPayment.getAccountNumber());
            event.put("amount",payment.getAmount());
            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,savedPayment.getId(),event);
            log.info("Payment failed: {}",savedPayment.getId());

        }catch (Exception e) {
            log.error("Error handling payment failed: {}",e.getMessage());
        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String,Object> entity = (Map<String,Object>) payload.get("payload");
        Map<String,Object> paymentWrapper = (Map<String, Object>) entity.get("payment");
        return (Map<String, Object>) paymentWrapper.get("entity");
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");
            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()->new RuntimeException("Payment not found for order id - "+orderId));
            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            Payment savedPayment = paymentRepository.save(payment);

            //Publish payment completed event
            Map<String,Object> event = new HashMap<>();
            event.put("paymentId",savedPayment.getId());
            event.put("accountNumber",savedPayment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("razorpayPaymentId",paymentId);
            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,savedPayment.getId(),event);
            log.info("Payment completed: {}",savedPayment.getId());
        } catch (Exception e) {
            log.error("Error handling payment success: {}",e.getMessage());
        }
    }

    public PaymentOrderResponse mapToResponse(Payment payment) {
        return PaymentOrderResponse.builder()
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .paymentId(payment.getId())
                .razorpayKeyId(keyId)
                .razorpayOrderId(payment.getRazorpayOrderId())
                .status(String.valueOf(payment.getStatus()))
                .build();
    }
}
