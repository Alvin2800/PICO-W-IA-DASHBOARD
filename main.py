from flask import Flask, request, jsonify
import mysql.connector
import os
from datetime import datetime

app = Flask(__name__)

# =========================
# CONNEXION MYSQL RAILWAY
# =========================




def get_db_connection():

    return mysql.connector.connect(
        host=os.environ.get("MYSQLHOST"),
        user=os.environ.get("MYSQLUSER"),
        password=os.environ.get("MYSQLPASSWORD"),
        database=os.environ.get("MYSQLDATABASE"),
        port=int(os.environ.get("MYSQLPORT", 3306))
    )

# =========================
# CREATION TABLE SQL
# =========================

def init_db():

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS fuel_measurements (

            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME,
            device_id VARCHAR(50),
            fuel_level FLOAT

        )

    """)

    conn.commit()

    cursor.close()
    conn.close()

# =========================
# ROUTE TEST
# =========================

@app.route("/")

def home():

    return "PICO W IA DASHBOARD API RUNNING 🚀"

# =========================
# ROUTE RECEPTION DATA
# =========================

@app.route("/test-db")
def test_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()

        return jsonify({"status": "success", "message": "Connexion MySQL OK"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
@app.route("/data")
def receive_data():
    try:
        device_id = request.args.get("device_id", "pico_001")
        fuel_level = request.args.get("fuel_level")

        if fuel_level is None:
            return jsonify({"error": "fuel_level manquant"}), 400

        fuel_level = float(fuel_level)

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fuel_measurements (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME,
                device_id VARCHAR(50),
                fuel_level FLOAT
            )
        """)

        cursor.execute("""
            INSERT INTO fuel_measurements (timestamp, device_id, fuel_level)
            VALUES (%s, %s, %s)
        """, (datetime.now(), device_id, fuel_level))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "status": "success",
            "device_id": device_id,
            "fuel_level": fuel_level
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# =========================
# ROUTE HISTORIQUE
# =========================

@app.route("/logs")

def logs():

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""

        SELECT *
        FROM fuel_measurements

        ORDER BY timestamp DESC

        LIMIT 50

    """)

    data = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(data)

# =========================
# LANCEMENT
# =========================

if __name__ == "__main__":

    init_db()

    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port
    )
