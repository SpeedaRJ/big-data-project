import xgboost as xgb


def make_fit_predict(X, y_train, X_test):
    clf = xgb.XGBRegressor(
        n_estimators=1000, max_depth=10, learning_rate=0.3, n_jobs=-1, random_state=42
    )
    clf.fit(X, y_train)
    return clf.predict(X_test)
