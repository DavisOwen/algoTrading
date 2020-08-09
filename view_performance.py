#!/usr/bin/env python
import os
from backtester.performance import PerformanceHandler
from backtester.utils import check_dir_exists


results_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           "results")
check_dir_exists(results_dir)
performance = PerformanceHandler(results_dir)
performance.load_results()
performance.plot_equity_curve()
performance.output_summary_stats()
