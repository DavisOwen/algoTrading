# algoTrading

## SETUP:

1. **Create env**

`python3 -m venv algotrading`

2. **Activate env**

`source algotrading/bin/activate`

3. **Install requirements**

`pip install -r requirements.txt`

4. **Download benchmark data**

https://www.wsj.com/market-data/quotes/index/SPX/historical-prices

Download csv data here with largest time frame, rename file to SP500.csv, and place in backtester/ folder

## Sphinx:

https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html

Sphinx autodocs will autogenerate documentation based on properly formatted comments. Look at current examples to get an idea of formatting

To regenerate docs:

1. make .rst file for any new modules 

2. run `make html`

## Credits

Follows the template for event driven backtester development outlined in this series

https://www.quantstart.com/articles/Event-Driven-Backtesting-with-Python-Part-I
