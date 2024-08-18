from sklearn.neural_network import MLPRegressor, MLPClassifier


def make_fit_predict(X, y_train, X_test):
    batch_size = 10
    X_batches = [X[i : i + batch_size] for i in range(0, len(X), batch_size)]
    y_batches = [
        y_train[i : i + batch_size] for i in range(0, len(y_train), batch_size)
    ]
    clf = MLPRegressor(
        max_iter=1000,
        tol=1e-3,
        random_state=42,
        hidden_layer_sizes=(128, 256, 512, 256, 128),
    )
    for X_batch, y_batch in zip(X_batches, y_batches):
        clf.partial_fit(X_batch, y_batch)
    return clf.predict(X_test)


def make_fit_predict_classification(X, y_train, X_test):
    batch_size = 10
    X_batches = [X[i : i + batch_size] for i in range(0, len(X), batch_size)]
    y_batches = [
        y_train[i : i + batch_size] for i in range(0, len(y_train), batch_size)
    ]
    clf = MLPClassifier(
        max_iter=1000,
        tol=1e-3,
        random_state=42,
        hidden_layer_sizes=(128, 256, 512, 256, 128),
    )
    for X_batch, y_batch in zip(X_batches, y_batches):
        clf.partial_fit(X_batch, y_batch, classes=[0, 1])
    return clf.predict(X_test), clf.predict_proba(X_test)
