import os
import logging
from queue import Empty
from .performance import PerformanceHandler
from .utils import EventType

# Logging and performance objects
performance = PerformanceHandler()
logger = logging.getLogger("backtester")


class Backtester(object):
    def __init__(self, events, bars, strategy, port, broker):
        self.events = events
        self.bars = bars
        self.strategy = strategy
        self.port = port
        self.broker = broker

    def start(self):
        logger.info("Starting Backtest")

        # Main Loop
        while True:
            # Update the bars (specific backtest code,
            # as opposed to live trading)
            if self.bars.continue_backtest:
                self.bars.update_bars()
            else:
                break

            # Handle the events
            while True:
                try:
                    event = self.events.get(block=False)
                except Empty:
                    break
                else:
                    if event is not None:
                        if event.type == EventType.MARKET:
                            self.strategy.calculate_signals(event)
                            self.port.update_timeindex(event)
                        elif event.type == EventType.SIGNAL:
                            self.port.update_signal(event)
                        elif event.type == EventType.ORDER:
                            logger.info(event)
                            self.broker.execute_order(event)
                        elif event.type == EventType.FILL:
                            logger.info(event)
                            self.port.update_fill(event)

        logger.info("Backtest completed")

    def show_performance(self):
        results = self.port.generate_results()
        performance.save_results(results)
        performance.plot_equity_curve()
        performance.output_summary_stats()
