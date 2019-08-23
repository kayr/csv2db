(ns fuzzy-sql.exporter
  (:require
    [fuzzy-sql.db :as db]
    [next.jdbc :as jdbc]
    [next.jdbc.sql :as sql])
  (:import (java.util Date)))

(declare unwind-values-to-keys)

(defn create-map-vk [[k values]]
  (if (coll? values)
    (reduce (fn [a v] (assoc a v k)) {} values)
    {values k}))

(defn unwind-values-to-keys [data-map]
  (reduce (fn [acc v] (into (create-map-vk v) acc)) {} data-map))


(def MYSQL-TO-JAVA-MAPPINGS {"varchar(255)"  [String]
                             "bigint"        [Integer Long BigInteger]
                             "numeric(19,6)" [Float Double BigDecimal]
                             "datetime"      [java.sql.Date Date]})


(def JAVA-TO-SQL-DB-MAPPING (unwind-values-to-keys MYSQL-TO-JAVA-MAPPINGS))


(defn filter-and-pair-with-index [pred? coll]
  (let [data-and-index (map-indexed vector coll)
        filtered-item-pairs (filter (fn [[_ item]] (pred? item)) data-and-index)]
    filtered-item-pairs))


(defn index-of [pred? coll]
  (let [[first-pair] (filter-and-pair-with-index pred? coll)
        [index] first-pair] index))


(defn get-header [csv] (first csv))


(defn get-header-index [csv name]
  (let [header (get-header csv)
        index (index-of #(= %1 name) header)] index))

(defn get-data [csv] (subvec csv 1 (count csv)))


(defn get-column [csv col-name]
  (let [index (get-header-index csv col-name)
        data (get-data csv)
        col-values (map #(nth %1 index) data)] col-values))


(defn detect-data-type [csv name]
  (let [col-values (get-column csv name)
        first-value (first (filter some? col-values))]
    (type first-value)))


(defn detect-db-type [csv name] (JAVA-TO-SQL-DB-MAPPING (detect-data-type csv name)))

(defn coalesce [a b] (if (nil? a) b a))

(defn generate-ddl [csv name]
  (let [headers (get-header csv)
        col-expr (map #(str "`" %1 "` " (detect-db-type csv %1)) headers)
        all-col-exprs (reduce #(str %1 "," %2) col-expr)]
    (str "create table `" name "` (" all-col-exprs ")")))

(defn to-map-list [csv]
  (let [[header & data] csv
        records (map #(zipmap header %1) data)]
    records))


(defn weigh [v]
  (cond
    (instance? String v) {:size (count v) :decimals 0}
    (instance? Number v) (let [[size decimals] (.split (str v) "\\.")]
                           {:size (count size) :decimals (count (coalesce decimals ""))})
    (instance? Date v) {:size 1 :decimals 0}
    :else {:size (count (str v)) :decimals 0}))


(defn- sum-weight [weight] (+ (:size weight) (:decimals weight)))


(defn detect-max-size-per-header [csv]
  (let [header (get-header csv)
        largest-per-header (map #(apply max
                                        (map sum-weight
                                             (map weigh (get-column csv %1))))
                                header)
        header-and-max (zipmap header largest-per-header)]
    header-and-max))


(defn create-table [ds csv name]
  (let [ddl-string (generate-ddl csv name)]
    (jdbc/execute! ds [ddl-string])))

(defn insert-data [ds csv name]
  (let [map-list (to-map-list csv)
        table-name (keyword name)]
    (sql/insert! ds table-name map-list)))



(defn create-or-insert [ds csv name]
  (let [table-record (db/get-tables ds name)
        table-count (count table-record)]
    (when (= 0 table-count)
      (create-table ds csv name))
    (insert-data ds csv name)))



(defn resize-if-necessary [ds csv table-name]
  (let [header-sizes (detect-max-size-per-header csv)]
    header-sizes))




; check if table exists
; create table
; for each record insert
; if fails to to adjust table
; insert again

