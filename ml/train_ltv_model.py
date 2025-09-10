import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import matplotlib.pyplot as plt

# Load Gold data
orders = pd.read_csv("data/silver/orders_clean.csv")
customers = pd.read_csv("data/silver/customers_clean.csv")

# Aggregate customer features
customer_features = orders.groupby("customer_id").agg(
    total_orders=("order_id", "count"),
    avg_order_value=("total_amount", "mean"),
    total_spent=("total_amount", "sum"),
).reset_index()

# Merge with customers
df = customer_features.merge(customers[["customer_id", "name"]], on="customer_id")

# Features (X) and target (y)
X = df[["total_orders", "avg_order_value"]]
y = df["total_spent"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = LinearRegression()
model.fit(X_train, y_train)

# Predictions
y_pred = model.predict(X_test)

# Evaluate
print("MSE:", mean_squared_error(y_test, y_pred))
print("R²:", r2_score(y_test, y_pred))

# Save model
joblib.dump(model, "ml/models/ltv_model.pkl")

# Plot predictions
plt.scatter(y_test, y_pred, alpha=0.5)
plt.xlabel("Actual LTV")
plt.ylabel("Predicted LTV")
plt.title("Customer LTV Prediction")
plt.savefig("ml/ltv_predictions.png")
plt.close()
