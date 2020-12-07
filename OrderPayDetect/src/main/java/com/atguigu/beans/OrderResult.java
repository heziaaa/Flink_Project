package com.atguigu.beans;

public class OrderResult {
    private Long orderId;
    private String status;

    public OrderResult() {
    }

    public OrderResult(Long orderId, String status) {
        this.orderId = orderId;
        this.status = status;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", status='" + status + '\'' +
                '}';
    }
}
