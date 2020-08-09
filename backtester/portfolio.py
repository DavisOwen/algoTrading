import pandas as pd
import copy

from abc import ABCMeta, abstractmethod
from math import floor

from .event import OrderEvent
from .utils import EventType


class Portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        raise NotImplementedError("Should implement update_signal()")

    @abstractmethod
    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent
        """
        raise NotImplementedError("Should implement update_fill()")


class NaivePortfolio(Portfolio):
    """
    The NaivePortfolio object is designed to send orders to
    a brokerage object witha constant quantity size blindly,
    i.e. wihtout any risk management or position sizing. It is
    used to test simpler strategies such as BuyAndHoldStrategy
    """
    def __init__(self, bars, events, start_date, leverage=1000,
                 initial_capital=100000.0):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        (USD unless otherwise stated).

        :param bars: The DataHandler object with current market data
        :type bars: DataHandler
        :param events: The Event Queue object
        :type events: Queue
        :param start_date: The start date (bar) of the portfolio.
        :type start_date: datetime
        :param initial_capital: The starting capital in USD.
        :type initial_capital: float
        """
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital
        self.leverage = leverage

        self.all_positions = []
        self.current_positions = self.construct_current_positions()

        self.all_holdings = []
        self.current_holdings = self.construct_current_holdings()

    def _update_symbol_list(self, symbol_list):
        """
        updates portfolio to use new symbol_list

        :param symbol_list: list of ticker symbols
        :type symbol_list: list(str)
        """
        self.symbol_list = symbol_list
        for s in symbol_list:
            if s not in self.current_positions:
                self.current_positions[s] = 0
            if s not in self.current_holdings:
                self.current_holdings[s]['cost'] = 0.0
                self.current_holdings[s]['cost_basis'] = 0.0

    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin

        :return: list of dictionary of positions
        :rtype: list(dict)
        """
        d = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        return [d]

    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.

        :return: list of dictionary of holdings
        :rtype: list(dict)
        """
        d = dict((s, {'cost': 0.0,  'cost_basis': 0.0}) for s
                 in self.symbol_list)
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_positions(self):
        return dict((k, v) for k, v in
                    [(s, 0) for s in self.symbol_list])

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.

        :return: dictionary of current holdings
        :rtype: dict
        """
        d = dict((s, {'cost': 0.0,  'cost_basis': 0.0}) for s
                 in self.symbol_list)
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d

    def get_current_positions(self):
        return self.current_positions

    def get_current_holdings(self):
        return self.current_holdings

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OLHCVI).

        Makes use of a MarketEvent form the events queue.

        :param event: MarketEvent object
        :type event: Event
        """
        if self.bars.symbol_list != self.symbol_list:
            self._update_symbol_list(self.bars.symbol_list)
        bars = {}
        for sym in self.symbol_list:
            bars[sym] = self.bars.get_latest_bars(sym, N=1)[0]

        # Update positions
        dp = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        dp['datetime'] = bars[self.symbol_list[0]]['Date']

        for s in self.symbol_list:
            dp[s] = self.current_positions[s]
            split = bars[s]['Split']
            if split != 1.0:
                dp[s] *= split

        # Append the current positions
        self.current_positions = copy.deepcopy(dp)
        self.all_positions.append(dp)

        # Update holdings
        dh = dict((s, {
            'cost': 0.0,
            'cost_basis': self.current_holdings[s]['cost_basis']})
                  for s in self.symbol_list)
        dh['datetime'] = bars[self.symbol_list[0]]['Date']
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * bars[s]['Close']
            dh[s]['cost'] = market_value
            dh['total'] += market_value

        self.current_holdings = copy.deepcopy(dh)
        # Append the current holdings
        self.all_holdings.append(dh)

    def update_positions_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the position matrix
        to reflect the new position

        :param fill: The FillEvent object to update the positions with.
        :type fill: FillEvent
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill.symbol] += fill_dir*fill.quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the holdings matrix
        to reflect the holdings value.

        :param fill: The FillEvent object to update the holdings with.
        :type fill: FillEvent
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update holdings list with new quantities
        cost = fill_dir * fill.fill_cost * fill.quantity
        self.current_holdings[fill.symbol]['cost'] += cost
        self.current_holdings[fill.symbol]['cost_basis'] = fill.fill_cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.

        Parameters:
        :param event: FillEvent object to update positions and holdings
        :type event: FillEvent
        """
        if event.type == EventType.FILL:
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """
        Simply transacts an OrderEvent object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        :param signal: The SignalEvent signal information
        :type signal: SignalEvent

        :return: order object
        :rtype: OrderEvent
        """
        order = None

        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength

        if direction != 'EXIT':
            mkt_quantity = floor(self.leverage * strength)
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')

        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY')
        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.

        :param event: SignalEvent object to update positions and holdings
        :type event: SignalEvent
        """
        if event.type == EventType.SIGNAL:
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    def generate_results(self):
        """
        Creates a dictionary with all holdings
        and all positions as Dataframes

        :return: dict of holdings and positions
        :rtype: dict
        """
        holdings = pd.DataFrame(self.all_holdings)
        holdings.set_index('datetime', inplace=True)
        positions = pd.DataFrame(self.all_positions)
        positions.set_index('datetime', inplace=True)
        holdings['returns'] = holdings['total'].pct_change()
        holdings['equity_curve'] = (1.0+holdings['returns']).cumprod()
        return {'holdings': holdings, 'positions': positions}

    def get_pnl(self, symbols):
        pnl = 0.0
        for s in symbols:
            bar = self.bars.get_latest_bars(s, N=1)[0]
            pnl += ((bar['Close'] - self.current_holdings[s]['cost_basis'])
                    * self.current_positions[s])
        return pnl
