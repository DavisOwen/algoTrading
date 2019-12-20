import os
import logging
from queue import Empty
from performance import PerformanceHandler

logger_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "logs")
results_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           "results")
# Logging and performance objects
performance = PerformanceHandler(results_dir)
performance.construct_logger(logger_dir)
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
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.port.update_timeindex(event)
                        elif event.type == 'SIGNAL':
                            self.port.update_signal(event)
                        elif event.type == 'ORDER':
                            self.broker.execute_order(event)
                        elif event.type == 'FILL':
                            self.port.update_fill(event)

        logger.info("Backtest completed")

    def show_performance(self):
        results = self.port.create_results_dataframe()
        performance.save_results(results)
        performance.plot_equity_curve()
        performance.output_summary_stats()
