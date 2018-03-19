package com.wemakeprice.anomalies.common;

import java.io.Serializable;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.common<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 13. AM 10:55<br/>
 * Description:
 */
public class AnomaliesCoupon implements Serializable{
    String payment_id;
    String order_time;
    String deal_id;
    int sale_coupon_org;

    public AnomaliesCoupon() {
    }

    public AnomaliesCoupon(String deal_id, String order_time, int sale_coupon_org) {
        this.payment_id = deal_id;
        this.order_time = order_time;
        this.sale_coupon_org = sale_coupon_org;
    }

    public String getPayment_id() {
        return payment_id;
    }

    public void setPayment_id(String payment_id) {
        this.payment_id = payment_id;
    }

    public String getDeal_id() {
        return deal_id;
    }

    public void setDeal_id(String deal_id) {
        this.deal_id = deal_id;
    }

    public String getOrder_time() {
        return order_time;
    }

    public void setOrder_time(String order_time) {
        this.order_time = order_time;
    }

    public long getSale_coupon_org() {
        return sale_coupon_org;
    }

    public void setSale_coupon_org(int sale_coupon_org) {
        this.sale_coupon_org = sale_coupon_org;
    }
}
