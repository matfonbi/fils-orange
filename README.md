# 🌍 Fils Orange – Air Quality ETL Pipeline (Open-Meteo)

## 🎯 Objectif du projet

L’objectif de ce projet est de construire un **pipeline ETL automatisé** permettant de :
- récupérer des **données de qualité de l’air et de météo** pour la ville de Paris,
- constituer un **historique fiable et à jour**,
- stocker ces données dans **Google BigQuery**,
- automatiser la mise à jour **quotidienne** sans intervention manuelle.

---

## 🧱 Architecture

Cloud Scheduler → Cloud Run Job → Open-Meteo API → BigQuery

---

## 📁 Structure du projet

```
config/
data/
 ├─ raw/
 └─ clean/
src/
 ├─ extract/
 ├─ transform/
 ├─ pipeline/
 └─ utils/
main.py
requirements.txt
README.md
```

---

## 🔄 Pipeline ETL

### Extraction
- APIs Open-Meteo (air quality + météo)
- Historique depuis janvier 2024
- Exécution quotidienne pour J-1

### Transformation
- Agrégation journalière
- Fusion air + météo
- Nettoyage des données

### Chargement
- BigQuery (MERGE pour éviter les doublons)

---

## ☁️ Déploiement Cloud

- **Cloud Run Job** : exécution batch Python
- **Cloud Scheduler** : déclenchement quotidien (06:00 Europe/Paris)
- **IAM / ADC** : authentification sécurisée

---

## 📊 BigQuery

- Dataset : `air_quality`
- Table : `historical_data`

---

## 🚀 Exécution locale (optionnelle)

```bash
python -m src.extract.extract_historical
python -m src.transform.transform_historical
```

---

## 🏁 Conclusion

Pipeline ETL cloud automatisé, scalable et conforme aux bonnes pratiques data engineering.
