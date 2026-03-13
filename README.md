# AMPECO Integration Project

This project is a Python-based data integration solution for the AMPECO platform. It features a modular architecture organized by business domains, allowing for independent maintenance and scaling of each data pipeline.

---

## 📂 Project Structure

The project follows a modular pattern within the `src/` directory, where each application implements its own **ETL (Extract, Transform, Load)** logic:

* **`configs/`**: Centralizes global configurations and project constants.
* **`inventory_application/`**: Processes asset inventory data.
* **`sessions_application/`**: Manages charging sessions and transaction data.
* **`users_application/`**: Handles user data integration and synchronization.
* **`repository/`**: Shared data access layer (database/storage) used across applications.

### Module Anatomy
Each sub-application is structured as follows:
* `ingestion/`: Data capture scripts (Ampeco API).
* `transform/`: Business logic, data cleaning, and normalization.
* `load/`: Persistence logic for the final destination.
* `main.py`: Specific entry point for the module.

---

## 🛠️ Prerequisites

* **Environment:** Linux (WSL: Ubuntu recommended).
* **Language:** Python 3.x.
* **Dependencies:** Managed via `pip`.

---

## ⚙️ Setup and Installation

1. **Environment Variables:**

   The project uses a `.env` file at the root to manage sensitive credentials. Ensure it is properly configured:
   ```bash
   # Create your .env file based on the local environment requirements
   touch .env

2. **Install Dependencies:**

    ```bash
    pip install pydantic-settings
    
    pip install -r requirements.txt
    ```

3. **Usage:**

    To run each integration individually, execute the corresponding main.py module from the project root:

    3.1. Run Inventory Pipeline:

    ```bash
    python3 src/inventory_application/main.py
    ```

    3.2. Run Users Pipeline:

    ```bash
    python3 src/users_application/main.py
    ```

    3.3. Run Sessions Pipeline:

    ```bash
    python3 src/sessions_application/main.py
    ```