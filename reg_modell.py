import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.dummy import DummyRegressor
import matplotlib.pyplot as plt
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
import joblib

# 1. Daten laden
df = pd.read_csv("gcs_features.csv")

# 2. Zeitlich sortieren und Zielvariablen erzeugen
df = df.sort_values(by=["subject_id", "window_start"])
df["gcs_t+1"] = df.groupby("subject_id")["gcs_total"].shift(-1)
df["gcs_t+2"] = df.groupby("subject_id")["gcs_total"].shift(-2)
df["gcs_t+3"] = df.groupby("subject_id")["gcs_total"].shift(-3)

# 3. Nur Zeilen mit vollständigem Ziel behalten
df = df.dropna(subset=["gcs_t+1", "gcs_t+2", "gcs_t+3"])

# 4. Fehlende Werte in Features mit Median auffüllen
features = ['heart_rate', 'systolic_bp', 'diastolic_bp', 'temperature', 'creatinine', 'hemoglobin']
df[features] = df[features].fillna(df[features].median())

# 5. Features normalisieren
X = df[features]
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# 6. Modelltraining für jede Zielzeit
targets = ["gcs_t+1", "gcs_t+2", "gcs_t+3"]
for target in targets:
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.3, random_state=42
    )

    # 7. BASELINE-Modell: Mittelwert-Prediction
    baseline = DummyRegressor(strategy="mean")
    baseline.fit(X_train, y_train)
    joblib.dump(baseline, f"baseline_model_{target}.pkl")
    y_base = baseline.predict(X_test)

    base_mse = mean_squared_error(y_test, y_base)
    base_r2 = r2_score(y_test, y_base)

    print(f"\nBaseline GCS Vorhersage für {target}")
    print(f"MSE: {base_mse:.2f}")
    print(f"R²: {base_r2:.2f}")

    # 8. Deep Learning Modell
    model = Sequential([
        Dense(64, activation='relu', input_shape=(X_train.shape[1],)),
        Dropout(0.3),
        Dense(32, activation='relu'),
        Dropout(0.2),
        Dense(1)
    ])

    model.compile(optimizer='adam', loss='mse')
    model.fit(X_train, y_train, epochs=50, batch_size=16, verbose=0)
    model.save(f"dl_model_{target}.h5")

    # 9. Evaluation
    y_pred = model.predict(X_test).flatten()
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\nDeep Learning GCS Vorhersage für {target}")
    print(f"MSE: {mse:.2f}")
    print(f"R²: {r2:.2f}")

    # 10. Visualisierung
    plt.figure()
    plt.scatter(y_test, y_pred, alpha=0.4, label='DL Vorhersage')
    plt.scatter(y_test, y_base, alpha=0.3, label='Baseline', marker='x')
    plt.plot([y.min(), y.max()], [y.min(), y.max()], 'r--', label='Ideal')
    plt.title(f'GCS Vorhersage: {target}')
    plt.xlabel('Tatsächlicher GCS')
    plt.ylabel('Vorhergesagter GCS')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()