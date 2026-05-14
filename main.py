from flask import Flask, request, jsonify
import mysql.connector
import os
from datetime import datetime
import pandas as pd
from sklearn.ensemble import IsolationForest
import google.generativeai as genai

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
# INITIALISATION DB
# =========================

def init_db():
    try:
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
        print("Table fuel_measurements OK")

    except Exception as e:
        print("Erreur init DB :", e)

init_db()

# =========================
# ACCUEIL
# =========================

@app.route("/")
def home():
    return "PICO W IA DASHBOARD API RUNNING"

# =========================
# TEST DB
# =========================

@app.route("/test-db")
def test_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify({
            "status": "success",
            "message": "Connexion MySQL OK",
            "result": result
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# =========================
# RECEPTION DONNEES PICO W
# =========================

@app.route("/data")
def receive_data():
    try:
        device_id = request.args.get("device_id", "pico_001")
        fuel_level = request.args.get("fuel_level")

        if fuel_level is None:
            return jsonify({
                "status": "error",
                "message": "fuel_level manquant"
            }), 400

        fuel_level = float(fuel_level)

        conn = get_db_connection()
        cursor = conn.cursor()

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
# HISTORIQUE
# =========================

@app.route("/logs")
def logs():
    try:
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

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# =========================
# ANALYSE ISOLATION FOREST
# =========================

@app.route("/analyze")
def analyze():
    try:
        conn = get_db_connection()

        query = """
            SELECT *
            FROM fuel_measurements
            ORDER BY timestamp ASC
        """

        df = pd.read_sql(query, conn)
        conn.close()

        if len(df) < 5:
            return jsonify({
                "status": "error",
                "message": "Pas assez de donnees pour lancer IsolationForest"
            })

        model = IsolationForest(
            contamination=0.15,
            random_state=42
        )

        df["anomaly"] = model.fit_predict(df[["fuel_level"]])

        results = []

        for _, row in df.iterrows():
            results.append({
                "id": int(row["id"]),
                "timestamp": str(row["timestamp"]),
                "device_id": row["device_id"],
                "fuel_level": float(row["fuel_level"]),
                "status": "ANOMALIE" if row["anomaly"] == -1 else "NORMAL"
            })

        return jsonify(results)

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# =========================
# RAPPORT IA AVEC GEMINI
# =========================

@app.route("/ai-report")
def ai_report():
    try:
        api_key = os.environ.get("GEMINI_API_KEY")

        if not api_key:
            return jsonify({
                "status": "error",
                "message": "GEMINI_API_KEY manquante dans Railway Variables"
            }), 500

        genai.configure(api_key=api_key)

        conn = get_db_connection()

        query = """
            SELECT *
            FROM fuel_measurements
            ORDER BY timestamp ASC
        """

        df = pd.read_sql(query, conn)
        conn.close()

        if len(df) < 5:
            return jsonify({
                "status": "error",
                "message": "Pas assez de donnees pour generer un rapport IA"
            })

        model_iforest = IsolationForest(
            contamination=0.15,
            random_state=42
        )

        df["anomaly"] = model_iforest.fit_predict(df[["fuel_level"]])

        anomalies = df[df["anomaly"] == -1]

        if anomalies.empty:
            return jsonify({
                "status": "success",
                "report": "Aucune anomalie carburant detectee. Le niveau de carburant semble stable."
            })

        anomaly_text = ""

        for _, row in anomalies.iterrows():
            anomaly_text += f"""
Horodatage: {row['timestamp']}
Appareil: {row['device_id']}
Niveau carburant: {row['fuel_level']} %
---
"""

        prompt = f"""
Tu es un assistant industriel specialise en IoT, supervision carburant,
detection d'anomalies et maintenance intelligente.

Voici les anomalies detectees par IsolationForest:

{anomaly_text}

Redige un rapport court en francais avec cette structure:

1. Resume de la situation
2. Interpretation possible
3. Recommandation technique

Le rapport doit etre clair, professionnel et facile a comprendre.
"""

        model_gemini = genai.GenerativeModel("gemini-1.5-flash")

        response = model_gemini.generate_content(prompt)

        report = response.text

        return jsonify({
            "status": "success",
            "report": report
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# =========================
# LANCEMENT LOCAL
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port
    )
