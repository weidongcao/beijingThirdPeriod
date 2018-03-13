package com.rainsoft.dao;

import com.google.common.base.Optional;
import com.rainsoft.domain.RegContentImChat;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface ImchatDao {
    List<RegContentImChat> getImchatBydate(String date);

    List<String[]> getImChatByHours(String startTime, String endTime);

    Long getMinIdFromDate(Optional<String> date);

    List<String[]> getDataById(Long id);
}
