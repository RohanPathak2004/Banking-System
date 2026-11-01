package com.banking.transactionservice.enums;



/*
 *
 * Transaction life cycle
 *
 * PENDING -> PROCESSING -> COMPLETED ( clean transaction )
 *                       -> PENDING_VERIFICATION ( suspicious detected)
 *                                               -> COMPLETED ( verified )
 *                                               -> FLAGGED ( SAGA REFUND )
 *                       -> FAILED
 *                       -> FLAGGED
 * */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    PENDING_VERIFICATION,
    FAILED,
    FLAGGED
}
