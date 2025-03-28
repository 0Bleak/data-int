# Aperçu du Projet

Ce projet consiste à **nettoyer**, **transformer** et **intégrer** plusieurs jeux de données (CSV, XML, JSON) relatifs à la consommation d’eau, aux crises hydriques et aux politiques de gestion. L’objectif est de charger ces données dans **plusieurs tables** d’une base PostgreSQL, afin de faciliter les requêtes et l’analyse future.

---

## Sources de Données

Nous avons travaillé avec **trois jeux de données** au format CSV, XML et JSON :

- **cleaned_global_water_consumption.csv (CSV)**  
  Données de consommation d’eau (par pays et par année), incluant la répartition par secteur (agricole, industriel, domestique), les précipitations et le taux d’épuisement des nappes.

- **water_crisis.xml (XML)**  
  Informations sur la gravité de la crise (CrisisSeverity) et les efforts de mitigation (MitigationEfforts).

- **water_policies.json (JSON)**  
  Politiques de conservation, montants d’investissement (Investment) et réglementations associées (Regulations).

Chaque source apporte des informations différentes mais complémentaires sur la consommation d’eau, la sévérité de la crise et les politiques mises en place.

---

## Étapes du Workflow

### Étape 1 : Chargement & Standardisation
**Script :** `load_and_standardize.py`

**Pourquoi ?**
- Les données proviennent de **formats différents** (CSV, XML, JSON).
- Nécessité d’homogénéiser les noms de colonnes et les types de données (texte, nombres, etc.).

**Qu’avons-nous fait ?**
- Chargé chaque fichier (CSV, XML, JSON) dans des DataFrames pandas.
- Standardisé les noms de colonnes (par ex. “Low”, “Medium”, “High” → 1, 2, 3).
- Converti les champs pays et dates pour qu’ils soient cohérents entre les sources.

**Résultat :**
- Des fichiers CSV, XML et JSON nettoyés et uniformisés, prêts pour un nettoyage approfondi.

---

### Étape 2 : Nettoyage des Données
**Script :** `clean_data.py`

**Pourquoi ?**
- Les valeurs manquantes et incohérentes faussent l’analyse.
- Les doublons créent des biais dans les agrégations.
- Certains champs (ex. investissement) présentaient des problèmes de format.

**Qu’avons-nous fait ?**
- Remplacé les champs vides ou aberrants (ex. “Unknown” pour mitigation efforts).
- Supprimé les doublons pour assurer l’unicité des enregistrements.
- Ajusté les valeurs (arrondi des montants, contrôle des types numériques).

**Résultat :**
- Des jeux de données dédoublonnés et sans valeurs manquantes critiques.

---

### Étape 3 : Transformation des Données
**Script :** `transform_data.py`

**Pourquoi ?**
- Certains champs nécessitaient une transformation avant de pouvoir être chargés en base.
- Les listes JSON (ex. “Regulations”) devaient être converties en texte simple.

**Qu’avons-nous fait ?**
- Converti les tableaux JSON en chaînes séparées par des virgules (“Agriculture, Industry, Household”).
- Vérifié le format numérique (ex. pourcentages, montants).
- Harmonisé à nouveau les noms de colonnes.

**Résultat :**
- Des données prêtes pour l’intégration, cohérentes et sans format non gérable par PostgreSQL.

---

### Étape 4 : Agrégation ou Fusion (Optionnel)
**Script :** `merge_data.py`

**Pourquoi ?**
- Les données partagent des clés communes (Country, Year), mais couvrent des aspects différents (consommation, crise, politiques).
- Besoin d’une **vue d’ensemble** pour certaines analyses globales.

**Qu’avons-nous fait ?**
- Joint les datasets sur *(Country, Year)* pour obtenir un fichier final (`final_water_data.csv`).
- Vérification des donnée pour qu'elles ne soient pas perdues (vérification des valeurs manquantes).

**Résultat :**
- Un fichier combiné pour ceux qui souhaitent une vision “tout-en-un”.
- **Cependant**, dans PostgreSQL, chaque table (consommation, crise, politiques, indicateurs) reste **distincte**.

---

### Étape 4.5 : Calcul des Indicateurs
**Script :** `calculate_indices.py`

**Pourquoi ?**
- Nécessité de créer des indicateurs synthétiques pour évaluer la pression sur la ressource eau.
- Exemples : ratio consommation/précipitations, stress lié aux nappes phréatiques, etc.

**Qu’avons-nous fait ?**
- Ouverture de nos fichiers CSV pertinent.
- Calculé plusieurs ratios, par exemple :  
  - `consumption_rainfall_ratio` = (Consommation Totale) / (Pluviométrie)  
  - `investment_consumption_ratio` = (Investissement) / (Consommation)  
  - `groundwater_stress` = (PerCapitaWaterUse) * (GroundwaterDepletionRate)  
  - `usage_balance` = (AgriculturalUse + IndustrialUse + HouseholdUse)  
  - `water_stress_index` = Combinaison de plusieurs facteurs (CrisisSeverity, ratio consommation/pluie, stress sur les nappes, etc.)

**Résultat :**
- Un fichier enrichi (par ex. `enriched_water_data.csv`) contenant les indicateurs, prêt à être chargé en base.

---

### Étape 5 : Configuration de la Base de Données & Insertion
**Script :** `load_to_postgres.py`

**Pourquoi ?**
- Un **entrepôt de données relationnel** facilite les requêtes et la visualisation.
- PostgreSQL est robuste et supporte des analyses complexes.

**Qu’avons-nous fait ?**
- (Optionnel) Supprimé l’ancienne base `global_water` pour partir d’une base propre.
- Créé **plusieurs tables** dans PostgreSQL :  
  1. `global_water_consumption` ou `water_data` (consommation d’eau)  
  2. `water_crisis` (gravité de la crise)  
  3. `water_policies` (politiques et investissements)  
  4. `indicators` (ratios et indices calculés)  
  5. Éventuellement, une table agrégée pour une vue globale.  
- Inséré les données ligne par ligne dans chacune de ces tables, en respectant les types de colonnes.

**Résultat :**
- **Plusieurs tables** relationnelles dans PostgreSQL, prêtes pour l’analyse.
- Données nettoyées, transformées et normalisées, accessibles via des requêtes SQL ou via Grafana.

## Lancement du Projet

1. **Cloner le dépôt**  
   ```bash
   git clone https://github.com/0Bleak/data-int.git
   cd data-int
   ```

1. **Démarrer les conteneurs**
    ```bash
    docker-compose up --build
    ```

Cela lance à la fois PostgreSQL et l’application qui exécute les scripts de chargement des données, et expose notre grafana pour visualisation des métrics.