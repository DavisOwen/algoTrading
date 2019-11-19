class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inhereted) events, that will trigger further events in the
    trading infrastructure.
    """
    pass


class MarketEvent(Event):
    """
    Handles the event of receiving a new market update with
    corresponding bars.
    """
    def __init__(self):
        """
        Initialises the MarketEvent.
        """
        self.type = 'MARKET'


class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.
    """
    def __init__(self, symbol, datetime, signal_type, strength=None):
        """
        Initialises the SignalEvent.

        :param symbol: The ticker symbol, e.g. 'GOOG'.
        :type symbol: str
        :param datetime: The timestamp at which the signal was generated.
        :type datetime: datetime
        :param signal_type: 'LONG', 'SHORT', or 'EXIT'
        :type signal_type: str
        :param strength: amount of ticker to order
        :type strength: float
        """
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type
        self.strength = strength


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market or limit),
    quantity and a direction.
    """
    def __init__(self, symbol, order_type, quantity, direction):
        """
        Initialises the order type, setting whether it is
        a Market order ('MKT') or Limit order ('LMT'), has
        a quantity (integral) and its direction ('BUY' or
        'SELL').

        :param symbol: The instrument to trade
        :type symbol: str
        :param order_type: 'MKT' or 'LMT' for Market or Limit.
        :type order_type: str
        :param quantity: Non-negative integer for quantity.
        :type quantity: int
        :param direction: 'BUY' or 'SELL' for long or short.
        :type direction: str
        """
        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction

    def print_order(self):
        """
        Outputs the values within the Order.
        """
        print("Order: Symbol={symbol}, Type={order_type},\
              Quantity={quantity}, Direction={direction}"
              .format(self.symbol, self.order_type,
                      self.quantity, self.direction))


class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the comission of the trade from the brokerage.
    """
    def __init__(self, timeindex, symbol, exchange, quantity,
                 direction, fill_cost, commission=None):
        """
        Initialises the FillEvent object. Sets the symbol, exchange,
        quantity, direction, cost of fill and an optional
        commission.

        If commission is not provided, the Fill object will
        calculate it based on the trade size and Interactive
        Broker fees.

        :param timeindex: The bar-resolution when the order was filled.
        :type timeindex: datetime
        :param symbol: The instrument which was filled
        :type symbol: str
        :param exchange: The exchange where the order was filled.
        :type exchange: str
        :param quantity: The filled quantity.
        :type quantity: int
        :param direction: The direction of fill ('BUY' or 'SELL')
        :type direction: str
        :param fill_cost: The holdings value in dollars.
        :type fill_cost: float
        :param commission: - (optional) commission sent from IB.
        :type commission: float
        """

        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost

        # Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):
        """
        Calculate the fees of trading based on an Interactive
        Brokers fee structure for API, in USD.

        This does not include exchange or ECN fees.

        Based on "US API Directed Orders":
        https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2

        :return: simulated ib commission cost
        :rtype: float
        """
        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:  # Greater than 500
            full_cost = max(1.3, 0.008 * self.quantity)
        full_cost = min(full_cost, 0.5 / 100.0
                        * self.quantity * self.fill_cost)
        return full_cost
