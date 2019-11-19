#!/usr/bin/env python

from performance import PerformanceHandler
import os


results_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           "results")
performance = PerformanceHandler(results_dir)
performance.load_results()
performance.plot_equity_curve()
performance.output_summary_stats()
