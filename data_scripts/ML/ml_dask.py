from dask_ml.linear_model import LinearRegression, LogisticRegression


def make_fit_predict(X, y_train, X_test):
    lr = LinearRegression(solver_kwargs={"normalize":False})
    lr.fit(X, y_train)
    return lr.predict(X_test)


def make_fit_predict_classification(X, y_train, X_test):
    lr = LogisticRegression(solver_kwargs={"normalize":False})
    lr.fit(X, y_train)
    return lr.predict(X_test), lr.predict_proba(X_test)
