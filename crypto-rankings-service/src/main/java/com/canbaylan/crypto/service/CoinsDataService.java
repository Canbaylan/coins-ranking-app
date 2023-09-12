package com.canbaylan.crypto.service;

import com.canbaylan.crypto.model.*;
import com.canbaylan.crypto.utils.HttpUtils;
import io.github.dengliming.redismodule.redisjson.RedisJSON;
import io.github.dengliming.redismodule.redisjson.args.GetArgs;
import io.github.dengliming.redismodule.redisjson.args.SetArgs;
import io.github.dengliming.redismodule.redisjson.utils.GsonUtils;
import io.github.dengliming.redismodule.redistimeseries.DuplicatePolicy;
import io.github.dengliming.redismodule.redistimeseries.RedisTimeSeries;
import io.github.dengliming.redismodule.redistimeseries.Sample;
import io.github.dengliming.redismodule.redistimeseries.TimeSeriesOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.coyote.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class CoinsDataService {
    public static final String GET_COINS_API= "https://coinranking1.p.rapidapi.com/coins?referenceCurrencyUuid=yhjMzLPhuIDl&timePeriod=24h&tiers%5B0%5D=1&orderBy=marketCap&orderDirection=desc&limit=50&offset=0";
    public static final String REDIS_KEY_COINS = "coins";
    public static final String GET_COIN_HISTORY_API= "https://coinranking1.p.rapidapi.com/coin/";
    public static final String COIN_HISTORY_TIME_PERIOD_PARAM="/history?timePeriod=";
    public static final List<String> timePeriods = List.of("24h","7d","30d","3m","1y","3y","5y");

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private RedisJSON redisJSON;

    @Autowired
    private RedisTimeSeries redisTimeSeries;

    public void fetchCoins(){
      log.info("Inside fetchCoins()");
      ResponseEntity<Coins> coinsEntity =
                            restTemplate.exchange(GET_COINS_API,
                            HttpMethod.GET,
                            HttpUtils.getHttpEntity(),
                            Coins.class);
      storeCoinsToRedisJSON(coinsEntity.getBody());
    }
    public void fetchCoinHistory(){
        log.info("Inside fetchCoinHistory()");
        List<CoinInfo> allCoins = getAllCoinsFromRedisJSON();
        allCoins.forEach(coinInfo -> {
            timePeriods.forEach(s -> {
                fetchCoinHistoryForTimePeriod(coinInfo,s);
            });
        });
    }

    private void fetchCoinHistoryForTimePeriod(CoinInfo coinInfo, String timePeriod) {
        log.info("Fetching Coin history of {} for time period {}",coinInfo.getName(),timePeriod);
        String url = GET_COIN_HISTORY_API + coinInfo.getUuid()
                +COIN_HISTORY_TIME_PERIOD_PARAM + timePeriod;
        ResponseEntity<CoinPriceHistory> coinPriceHistoryResponseEntity =
                restTemplate.exchange(url,
                HttpMethod.GET,
                HttpUtils.getHttpEntity(),
                CoinPriceHistory.class);
        log.info("Data fetched from api for coin history");
        storeCoinHistoryToRedisTS(coinPriceHistoryResponseEntity.getBody(),
                coinInfo.getSymbol(),
                timePeriod);
    }

    private void storeCoinHistoryToRedisTS(CoinPriceHistory coinPriceHistory, String symbol, String timePeriod) {
        log.info("Storing Coin History of {} for Time Period {} into Redis TS", symbol, timePeriod);
        List<CoinPriceHistoryExchangeRate> coinExchangeRateData =
                coinPriceHistory.getData().getHistory();
        coinExchangeRateData.stream()
                .filter(ch -> ch.getPrice() != null && ch.getTimestamp() != null)
                .forEach(ch -> {
                    redisTimeSeries.add(new Sample(symbol + ":" + timePeriod, Sample.Value.of(Long.valueOf(ch.getTimestamp()),
                            Double.valueOf(ch.getPrice()))), new TimeSeriesOptions()
                            .unCompressed()
                            .duplicatePolicy(DuplicatePolicy.LAST));
                });
        log.info("Complete: Stored Coin History of {} for Time Period {} into Redis TS", symbol, timePeriod);
    }

    private List<CoinInfo> getAllCoinsFromRedisJSON() {
        CoinData coinData =
                redisJSON.get(REDIS_KEY_COINS,
                        CoinData.class,
                        new GetArgs().path(".data")
                                .indent("\t")
                                .newLine("\n")
                                .space(" "));
        log.info("allCoins: " + coinData);
        return coinData.getCoins();
    }

    private void storeCoinsToRedisJSON(Coins coins) {
        redisJSON.set(REDIS_KEY_COINS,
                SetArgs.Builder.create(".", GsonUtils.toJson(coins)));
    }

    public List<CoinInfo> fetchAllCoinsFromRedisJSON() {
        return getAllCoinsFromRedisJSON();
    }

    public List<Sample.Value> fetchCoinHistoryPerTimePeriodFromRedisTS(String symbol, String timePeriod) {
        Map<String,Object> tsInfo = fetchTSInfoForSymbol(symbol,timePeriod);
        Long firstTimeStamp = Long.valueOf(tsInfo.get("firstTimestamp").toString());
        Long lastTimeStamp = Long.valueOf(tsInfo.get("lastTimestamp").toString());
        List<Sample.Value> coinsTSData =
                fetchTSDataForCoin(symbol,timePeriod,firstTimeStamp,lastTimeStamp);
        return coinsTSData;
    }

    private List<Sample.Value> fetchTSDataForCoin(String symbol, String timePeriod, Long firstTimeStamp, Long lastTimeStamp) {
        String key = symbol + ":"+timePeriod;
        return redisTimeSeries.range(key,firstTimeStamp,lastTimeStamp);
    }

    private Map<String, Object> fetchTSInfoForSymbol(String symbol, String timePeriod) {
        return redisTimeSeries.info(symbol+":"+timePeriod);
    }
}
