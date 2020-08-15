package utils

object RptUtils {

    /**
      * List(原始请求，有效请求，广告请求)
      */
    def caculateReq(reqMode: Int, prcNode: Int): List[Double] = {
        if (reqMode == 1 && prcNode == 1) {
            List[Double](1, 0, 0)
        } else if (reqMode == 1 && prcNode == 2) {
            List[Double](1, 1, 0)
        } else if (reqMode == 1 && prcNode == 3) {
            List[Double](1, 1, 1)
        } else List[Double](0, 0, 0)
    }

    /**
      * List(参与竞价，竞价成功，消费，成本)
      */
    def caculateRtb(effTive: Int, bill: Int, bid: Int, orderId: Int, win: Int, winPrice: Double, adPayMent: Double): List[Double] = {

        if (effTive == 1 && bill == 1 && bid == 1 && orderId != 0) {
            List[Double](1, 0, 0, 0)
        } else if (effTive == 1 && bill == 1 && win == 1) {
            List[Double](0, 1, winPrice / 1000.0, adPayMent / 1000.0)
        } else List[Double](0, 0, 0, 0)
    }

    /**
      * List(广告展示，点击)
      */
    def caculateShowClick(reqMode: Int, effTive: Int): List[Double] = {
        if (reqMode == 2 && effTive == 1) {
            List[Double](1, 0)
        } else if (reqMode == 3 && effTive == 1) {
            List[Double](0, 1)
        } else List[Double](0, 0)

    }

}
