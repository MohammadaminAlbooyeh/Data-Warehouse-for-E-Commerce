import pandas as pd
import joblib

# Load model
model = joblib.load("ml/models/ltv_model.pkl")

# Example: Predict LTV for new customers
new_data = pd.DataFrame({
    "total_orders": [5, 10],
    "avg_order_value": [50, 120],
})

predictions = model.predict(new_data)
print("Predicted LTV:", predictions)
