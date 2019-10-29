import Queue
from data import QuandlDataHandler
from strategy import BollingerBandJohansenStrategy
from portfolio import NaivePortfolio
from broker import SimulatedExecutionHandler

events = Queue()
# TODO add parameters to objects
bars = QuandlDataHandler()
strategy = BollingerBandJohansenStrategy()
port = NaivePortfolio()
broker = SimulatedExecutionHandler()

while True:
    # Update the bars (specific backtest code, as opposed to live trading)
    if bars.continue_backtest:
        bars.update_bars()
    else:
        break

    # Handle the events
    while True:
        try:
            event = events.get(False)
        except Queue.Empty:
            break
        else:
            if event is not None:
                if event.type == 'MARKET':
                    strategy.calculate_signals(event)
                    port.update_timeindex(event)
                elif event.type == 'SIGNAL':
                    port.update_signal(event)
                elif event.type == 'ORDER':
                    broker.execute_order(event)
                elif event.type == 'FILL':
                    port.update_fill(event)
