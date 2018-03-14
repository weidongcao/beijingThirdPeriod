package com.rainsoft.inter;

import com.google.common.base.Optional;

import java.util.List;

/**
 * ISec管理综合平台Dao层的基类
 *
 * #Time 2018-03-14 11:14:07
 * Created by CaoWeiDong on 2018-03-14.
 */
public interface ISecDaoBaseInter extends BaseInter {
    //根据起始ID和步长从数据库配置查询数据
    List<String[]> getDataById(Optional<Long> id);
}
