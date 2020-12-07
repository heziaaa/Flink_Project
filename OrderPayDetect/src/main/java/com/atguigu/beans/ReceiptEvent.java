package com.atguigu.beans;

public class ReceiptEvent {
    private String txId;
    private String payChannle;
    private Long timestamp;

    public ReceiptEvent() {
    }

    public ReceiptEvent(String txId, String payChannle, Long timestamp) {
        this.txId = txId;
        this.payChannle = payChannle;
        this.timestamp = timestamp;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getPayChannle() {
        return payChannle;
    }

    public void setPayChannle(String payChannle) {
        this.payChannle = payChannle;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReceiptEvent{" +
                "txId='" + txId + '\'' +
                ", payChannle='" + payChannle + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
