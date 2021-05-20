import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
import scipy
import statsmodels
from sklearn import gaussian_process
from sklearn.gaussian_process.kernels import Matern, WhiteKernel, ConstantKernel

def gp(df, ct_var, q_var, q_val, plot=True):
    work_df = df[(df.q_var == q_var) & (df.ct_var==ct_var) & (df.q_val==q_val)]
    x = work_df.WEEK
    y = work_df.proportions
    kernel = ConstantKernel() + Matern(length_scale=2, nu=3/2) + WhiteKernel(noise_level=0.5)
    gp = gaussian_process.GaussianProcessRegressor(kernel=kernel)
    gp.fit(np.array(x).reshape(len(x),1), y)

    x_pred = np.linspace(work_df.WEEK.min(), work_df.WEEK.max()).reshape(-1,1)
    y_pred, sigma = gp.predict(x_pred, return_std=True)
    if not plot:
        return x_pred, y_pred, sigma
    plt.figure(figsize=(10,8))
    sns.regplot(x, y, fit_reg=False, label='Data')
    plt.plot(x_pred, y_pred, color='grey')
    plt.fill(np.concatenate([x_pred, x_pred[::-1]]),
            np.concatenate([y_pred - 2*sigma,
            (y_pred + 2*sigma)[::-1]]),
            alpha=.5, fc='grey', ec='None', label='95% CI')
    plt.xlabel('$Week$')
    plt.ylabel('$Proportion$')
    plt.legend(loc='lower left')


def loc_eval(x, b):
    loc_est = 0
    for i in enumerate(b): 
        loc_est+=i[1]*(x**i[0])
    return(loc_est)


def loess(xvals, yvals, data, alpha, poly_degree=1):
    '''
    lifted from Mike Lang
    https://github.com/MikLang/Lowess_simulation/blob/master/Lowess_simulations.py
    '''
    all_data = sorted(zip(data[xvals].tolist(), data[yvals].tolist()), key=lambda x: x[0])
    xvals, yvals = zip(*all_data)
    eval_df = pd.DataFrame(columns=['v','g'])
    n = len(xvals)
    m = n + 1
    q = int(np.floor(n * alpha) if alpha <= 1.0 else n)
    avg_interval = ((max(xvals)-min(xvals))/len(xvals))
    v_lb = min(xvals)-(.5*avg_interval)
    v_ub = (max(xvals)+(.5*avg_interval))
    v = enumerate(np.linspace(start=v_lb, stop=v_ub, num=m), start=1)
    xcols = [np.ones_like(xvals)]
    for j in range(1, (poly_degree + 1)):
        xcols.append([i ** j for i in xvals])
    X = np.vstack(xcols).T
    for i in v:
        iterpos = i[0]
        iterval = i[1]
        iterdists = sorted([(j, np.abs(j-iterval)) for j in xvals], key=lambda x: x[1])
        _, raw_dists = zip(*iterdists)
        scale_fact = raw_dists[q-1]
        scaled_dists = [(j[0],(j[1]/scale_fact)) for j in iterdists]
        weights = [(j[0],((1-np.abs(j[1]**3))**3 if j[1]<=1 else 0)) for j in scaled_dists]
        _, weights = zip(*sorted(weights, key=lambda x: x[0]))
        _, raw_dists = zip(*sorted(iterdists, key=lambda x: x[0]))
        _, scaled_dists = zip(*sorted(scaled_dists, key=lambda x: x[0]))
        W = np.diag(weights)
        b = np.linalg.inv(X.T @ W @ X) @ (X.T @ W @ yvals)
        local_est = loc_eval(iterval, b)
        iter_df2   = pd.DataFrame({
                       'v'  :[iterval],
                       'g'  :[local_est]
                       })
        eval_df = pd.concat([eval_df, iter_df2])
    eval_df = eval_df[['v','g']]
    return eval_df
