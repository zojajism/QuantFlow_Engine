import asyncio
from datetime import datetime
import json
from nats.aio.client import Client as NATS
from nats.js import api
import os
from logger_config import setup_logger
from database.insert_indicators import get_pg_pool
from NATS_setup import ensure_streams_from_yaml
from alert_manager import send_alert
import buffers.buffer_initializer as buffers
from buffers.candle_buffer import Keys
from strategies.engines import get_engine
from indicators.indicator_calculator import compute_and_append_on_close
from database.insert_indicators import insert_indicators_to_db_async

Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

async def main():

    logger = setup_logger()
    logger.info("Starting QuantFlow_Engine system...")
    logger.info(
                json.dumps({
                        "EventCode": 0,
                        "Message": f"Starting QuantFlow_Engine system..."
                    })
            )

    nc = NATS()
    await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
    await ensure_streams_from_yaml(nc, "streams.yaml")
    js = nc.jetstream()

    symbols = ["BTC/USDT", "ETH/BTC", "ETH/USDT"]
    timeframes = ["1m", "15m", "1h"]
    CANDLE_BUFFER, INDICATOR_BUFFER = buffers.init_buffers("Binance", symbols, timeframes)
    print("AA")
    # --- Consumer 1: Tick Engine
    try:
        await js.delete_consumer(Tick_STREAM, "tick-quant-engine")
    except Exception:
        pass
    
    await js.add_consumer(
        Tick_STREAM,
        api.ConsumerConfig(
            durable_name="tick-quant-engine",
            filter_subject=Tick_SUBJECT,
            ack_policy=api.AckPolicy.EXPLICIT,
            deliver_policy=api.DeliverPolicy.NEW,  
            max_ack_pending=5000,
        )
    )

    # --- Consumer 2: Candle Engine
    try:
        await js.delete_consumer(Candle_STREAM, "candle-quant-engine")
    except Exception:
        pass    

    await js.add_consumer(
        Candle_STREAM,
        api.ConsumerConfig(
            durable_name="candle-quant-engine",
            filter_subject=Candle_SUBJECT,
            ack_policy=api.AckPolicy.EXPLICIT,
            deliver_policy=api.DeliverPolicy.NEW, 
            max_ack_pending=5000,
        )
    )

    
    # Pull-based subscription for Tick Engine
    sub_Tick = await js.pull_subscribe(Tick_SUBJECT, durable="tick-quant-engine")
   
    # Pull-based subscription for Candle Engine
    sub_Candle = await js.pull_subscribe(Candle_SUBJECT, durable="candle-quant-engine")
    print("BB")

    async def tick_engine_worker():
        while True:
            try:
                msgs = await sub_Tick.fetch(100, timeout=1)
                for msg in msgs:
                    logger.info(f"Received from {msg.subject}")
                    
                    tick_data = json.loads(msg.data.decode("utf-8"))
                    tick_data["tick_time"] = datetime.fromisoformat(tick_data["tick_time"])
                    tick_data["message_datetime"] = datetime.fromisoformat(tick_data["insert_ts"])

                    #await insert_tickers_to_db_async(db_pool, tick_data)

                    await msg.ack()
            except Exception as e:
                logger.error(
                        json.dumps({
                                "EventCode": -1,
                                "Message": f"NATS error: Tick, {e}"
                            })
                    )
                await send_alert("NATS-Error-Tick", str(e))                
                await asyncio.sleep(0.05)
   

    async def candle_engine_worker():
        while True:
            try:
                msgs = await sub_Candle.fetch(100, timeout=1)
                print("CC")
                for msg in msgs:
                    logger.info(f"Received from {msg.subject}: ")
                    tokens = msg.subject.split(".")
                    timeframe = tokens[-1]

                    if timeframe in ("1m", "15m", "1h"):
                        
                        candle_data = json.loads(msg.data.decode("utf-8"))
                        candle_data["open_time"] = datetime.fromisoformat(candle_data["open_time"])
                        candle_data["close_time"] = datetime.fromisoformat(candle_data["close_time"])
                        candle_data["message_datetime"] = datetime.fromisoformat(candle_data["insert_ts"])
                        symbol = candle_data["symbol"]
                        key = Keys("Binance", symbol, timeframe)

                        # 1) append candle to CandleBuffer
                        buffers.CANDLE_BUFFER.append(key, candle_data)
                        
                        engine = get_engine("Binance", symbol)
                    

                        # 2) compute indicators from CandleBuffer and append to IndicatorBuffer
                        values = compute_and_append_on_close(
                            key=key,
                            close_time=candle_data["close_time"],
                            close_price=candle_data["close"]
                        )
                        logger.info(f"Indicator values for symbol: {symbol}, timeframe: {timeframe}, close time: {candle_data['close_time']}, Indicator count: {len(values.items())}")
                
                        # 3) Call DecisionEngine on candle close (update bias or trade)
                        close_ts = int(candle_data["close_time"].timestamp())

                        if timeframe in ("15m", "1h"):
                            # Update bias for HTFs on their own candle close
                            await engine.on_candle_close(tf=timeframe, ts=close_ts)

                        elif timeframe == "1m":
                            # Produce BUY/SELL/HOLD (entries only on 1m per v0.1) and paper-execute
                            final_signal = await engine.on_candle_close(tf=timeframe, ts=close_ts)
                            logger.info(f"[DE] {symbol} {timeframe} close @ {candle_data['close_time']} with price: {candle_data['close']},  signal={final_signal}, balance={engine.portfolio.balance_usd}, quantity={engine.portfolio.position_qty}")
                        #3)=================================================================================================================

                        # 4) Insert indicators into Database
                        await insert_indicators_to_db_async(
                            exchange="Binance",
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=candle_data["close_time"],
                            close_price=candle_data["close"],
                            results=values,
                        )
                        
                    await msg.ack()

            except Exception as e:
                '''
                logger.error(
                        json.dumps({
                                "EventCode": -1,
                                "Message": f"NATS error: Candle, {e}"
                            })
                    )
                '''    
                #await send_alert("NATS-Error-Candle", str(e))                
                await asyncio.sleep(0.05)
    
    logger.info(
            json.dumps({
                         "EventCode": 0,
                         "Message": f"Subscriber Quant Engine starts...."
                    })
            )
   
   
    await asyncio.gather(candle_engine_worker())
    
    
if __name__ == "__main__":
    asyncio.run(main())
