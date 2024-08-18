from dask_ml.linear_model import LinearRegression


def make_fit_predict(X, y_train, X_test):
    lr = LinearRegression()
    lr.fit(X, y_train)
    return lr.predict(X_test)
