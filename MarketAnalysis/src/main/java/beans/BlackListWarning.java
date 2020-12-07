package beans;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.arguigu.market_analysis.beans
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/31 11:28
 */

/**
 * @ClassName: BlackListWarning
 * @Description:
 * @Author: wushengran on 2020/10/31 11:28
 * @Version: 1.0
 */
public class BlackListWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;

    public BlackListWarning() {
    }

    public BlackListWarning(Long userId, Long adId, String warningMsg) {
        this.userId = userId;
        this.adId = adId;
        this.warningMsg = warningMsg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "BlackListWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
