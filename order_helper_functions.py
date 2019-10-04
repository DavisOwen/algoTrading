def adjustHedgeRatio(hedge):
    """
    Adjusts the HedgeRatio to account for slippage model (Volume Limit)

    Will not order unless the order can be filled the same day

    Adjusts for the lowest common denominator
    """
    volume = context.volume.loc[pd.to_datetime(context.get_datetime()).date()]
    for i, tick in enumerate(context.tickers):
        max_order = volume[context.tick_list[i]]*0.025
        adj_hedge = (max_order*data.current(tick, 'price')) \
            / context.portfolio.portfolio_value
        order_size = abs(context.portfolio.portfolio_value * hedge[i]) \
            / data.current(tick, 'price')
        if order_size > max_order:
            hedge = adjustHedgeRatio(hedge*adj_hedge)
            break
    return hedge


def computeCost(S):
    """
    Computes cost function given the number of shares S
    """
    numerator = data.current(context.tickers, 'price')*S
    denominator = np.sum(numerator)
    numerator -= np.absolute(context.hedgeRatio)
    cost = numerator/denominator
    cost = np.sum(cost**2)
    return cost


def order_target_portfolio_percentages(order_type):
    """
    Use gradient descent to minimize difference function between
    portfolio weights and hedge ratio
    """
    shares = (context.portfolio.portfolio_value*context.hedgeRatio) / \
        data.current(context.tickers, 'price')
    shares = np.absolute(shares.values)
    A = np.dot(data.current(context.tickers, 'price'), shares)
    cost = computeCost(shares)
    alpha = 10000
    while cost >= 0.14:
        print('Current Cost Function = ', cost)
        for i, tick in enumerate(context.tickers):
            new_tickers = context.tickers[:i] + context.tickers[i+1:]
            new_shares = np.delete(shares, i, 0)
            new_hedgeRatio = np.delete(np.absolute(context.hedgeRatio), i, 0)
            Pi = data.current(tick, 'price')
            B = data.current(new_tickers, 'price')*new_shares
            C = np.sum(B)
            gradient = (Pi * (shares[i] * C + np.sum(B**2)))/A**3 \
                - (np.absolute(context.hedgeRatio[i]) * C
                   + (Pi * np.dot(new_hedgeRatio, B)))/A**2
            shares[i] -= alpha*gradient
        cost = computeCost(shares)
    shares[context.hedgeRatio < 0] *= -1
    for i, tick in enumerate(context.tickers):
        if order_type == 'long':
            order(tick, shares[i])
        if order_type == 'short':
            order(tick, -shares[i])
