package com.trading.engine.cluster;

import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;

/**
 * Immutable in-memory representation of an active order held by the cluster {@link OrderBook}.
 *
 * <p>All numeric fields are fixed-point with scale {@code 10^-8} (matching the SBE wire format).
 *
 * <p>Enum types are reused from the generated SBE codecs to avoid translation cost when an order is
 * admitted into the book directly from a decoded NewOrderSingle.
 *
 * @param clOrdId client order id (FIX tag 11)
 * @param symbol instrument symbol (FIX tag 55)
 * @param side buy/sell (FIX tag 54)
 * @param ordType market/limit/etc. (FIX tag 40)
 * @param price limit price, fixed-point 10^-8 (FIX tag 44; 0 for market orders)
 * @param orderQty order quantity, fixed-point 10^-8 (FIX tag 38)
 * @param timeInForce day/GTC/IOC/FOK (FIX tag 59)
 * @param account account code (FIX tag 1)
 */
public record OrderState(
    String clOrdId,
    String symbol,
    SideEnum side,
    OrdTypeEnum ordType,
    long price,
    long orderQty,
    TimeInForceEnum timeInForce,
    String account) {}
