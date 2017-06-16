package com.rainsoft.dao;

import com.rainsoft.domain.RegContentHttp;
import com.rainsoft.domain.RegContentImChat;

import java.util.List;

/**
 * Created by Administrator on 2017-06-12.
 */
public interface ImchatDao {
    List<RegContentImChat> getImchatBydate(String date);

}
