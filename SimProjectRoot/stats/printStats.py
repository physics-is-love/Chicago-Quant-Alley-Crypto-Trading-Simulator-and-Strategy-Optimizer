import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def printStats(pnl_history):
    df = pd.DataFrame({'PnL': pnl_history})
    df['Returns'] = df['PnL'].pct_change().fillna(0)
    sharpe = df['Returns'].mean() / df['Returns'].std() * np.sqrt(252)

    cum_pnl = df['PnL'].cumsum()
    drawdown = cum_pnl - cum_pnl.cummax()
    max_drawdown = drawdown.min()

    var_95 = df['Returns'].quantile(0.05)
    es_95 = df['Returns'][df['Returns'] <= var_95].mean()

    print("Mean PnL:", df['PnL'].mean())
    print("Median PnL:", df['PnL'].median())
    print("Sharpe Ratio:", sharpe)
    print("Max Drawdown:", max_drawdown)
    print("VaR (95%):", var_95)
    print("ES (95%):", es_95)

    cum_pnl.plot(title='Cumulative PnL')
    plt.savefig('cumulative_pnl.png')
    plt.clf()

    drawdown.plot(title='Drawdown')
    plt.savefig('drawdown.png')
