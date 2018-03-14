package com.rainsoft.dao;

import com.rainsoft.domain.RegContentImChat;
import com.rainsoft.inter.ContentDaoBaseInter;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface ImchatDao extends ContentDaoBaseInter {
    List<RegContentImChat> getImchatBydate(String date);

    List<String[]> getImChatByHours(String startTime, String endTime);
}
