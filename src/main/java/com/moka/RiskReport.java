package com.moka;

/**
 * 数据格式:orgId  updated_at data，\t分隔
 * 例如：test  1582251501(时间戳，单位:秒) Ngmmms
 * 使用Java实现下面的程序逻辑
 * 方法1. 接收一条数据为参数，实现：同一个orgId在一秒钟内（updated_at）出现次数超过50次记为一次风险，如果在连续的60秒
 * （比如当前数据的时间1582251501，连续60指的updated_at时间是1582251441 ~ 1582251501的数据）内风险次数大于40次，打印：{orgId}异常活跃
 *
 * @author zhangbaoming
 * @date 2021/3/17 3:38 下午
 */
public class RiskReport {

}
