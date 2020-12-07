package beans;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.arguigu.market_analysis.beans
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/31 10:32
 */

/**
 * @ClassName: AdCountByProvince
 * @Description:
 * @Author: wushengran on 2020/10/31 10:32
 * @Version: 1.0
 */
public class AdCountByProvince {
    private String province;
    private String windowEnd;
    private Long count;

    public AdCountByProvince() {
    }

    public AdCountByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdCountByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }
}
