# smart-grid-sustainability-agent
An end-to-end Data Engineering pipeline on Databricks using Medallion Architecture and Mosaic AI to optimize industrial energy consumption.

# ⚡ Smart-Grid Sustainability Agent
I built this project to show how we can use **Databricks** and **AI** to save money on electricity while being more "Green."

### 🚀 What this project does:
1. **Pulls Live Data:** It talks to a weather API to get wind and solar forecasts for West Texas.
2. **Predicts Prices:** It uses a "Silver Layer" to guess when electricity will be cheapest based on the weather.
3. **Finds the Best Times:** It creates a "Gold Layer" that ranks the top 5 hours to run heavy machinery.
4. **AI Ready:** The final data is connected to a Databricks Genie AI, so a manager can just ask: *"When should I start the assembly line?"*

### 🛠️ Tools I used:
- **Databricks** (The main platform)
- **PySpark** (For processing the data)
- **Delta Lake** (To store the tables safely)
- **SQL** (To create reports)
- **Unity Catalog** (To keep the data organized)
