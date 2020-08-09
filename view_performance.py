#!/usr/bin/env python
import os
from backtester.performance import PerformanceHandler

performance = PerformanceHandler()
performance.load_results()
performance.plot_equity_curve()
performance.output_summary_stats()
