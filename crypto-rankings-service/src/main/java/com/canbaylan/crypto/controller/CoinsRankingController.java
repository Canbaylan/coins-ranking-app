package com.canbaylan.crypto.controller;

import com.canbaylan.crypto.model.CoinInfo;
import com.canbaylan.crypto.model.HistoryData;
import com.canbaylan.crypto.service.CoinsDataService;
import com.canbaylan.crypto.utils.Utility;
import io.github.dengliming.redismodule.redistimeseries.Sample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/coins")
@Slf4j
@CrossOrigin(value="http://localhost:3000")
public class CoinsRankingController {
    @Autowired
    private CoinsDataService coinsDataService;

    @GetMapping
    public ResponseEntity<List<CoinInfo>> fetchAllCoins(){
        return ResponseEntity.ok()
                .body(coinsDataService.fetchAllCoinsFromRedisJSON());
    }

    @GetMapping("/{symbol}/{timePeriod}")
    public List<HistoryData> fetchCoinHistoryPerTimePeriod(
            @PathVariable String symbol,
            @PathVariable String timePeriod){
            List<Sample.Value> coinsTSData
                    =coinsDataService.fetchCoinHistoryPerTimePeriodFromRedisTS(symbol,timePeriod);
            List<HistoryData> coinHistory =
                    coinsTSData.stream()
                            .map(value -> new HistoryData(
                                    Utility.convertUnixTimeToDate(value.getTimestamp()),
                                    Utility.round(value.getValue(),2)
                            ))
                            .collect(Collectors.toList());
            return coinHistory;

    }
}
