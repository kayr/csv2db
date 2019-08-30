(ns fuzzy-sql.exporter
  (:require
    [fuzzy-sql.db :as db]
    [next.jdbc :as jdbc]
    [next.jdbc.sql :as sql])
  (:import (java.util Date)))


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
                           {:size (count size) :decimals (count (or decimals ""))})
    (instance? Date v) {:size 1 :decimals 0}
    :else {:size (count (str v)) :decimals 0}))


(defn create-table! [ds csv name]
  (let [ddl-string (generate-ddl csv name)]
    (jdbc/execute! ds [ddl-string])))

(defn do-with-retry [do-fn correct-fn]
  (try (do-fn)
       (catch Exception _
         (do (correct-fn) (do-fn)))))


(defn left-join-pair [join-fn col1 col2]
  (map (fn [val1] {:left  val1
                   :right (some (fn [val2] (when (join-fn val1 val2) val2)) col2)}) col1))

(defn get-mysql-data-type [type size decimals]
  (str type "(" (if (> decimals 0) (str size "," decimals) (str size)) ")"))

(defn resize-column [{col-name :name :keys [size decimals :type table-name]}]
  (str "ALTER TABLE `" table-name "` MODIFY " (name col-name) " " (get-mysql-data-type type size decimals)))

(defn add-column [{col-name :name :keys [size decimals type table-name]}]
  (str "ALTER TABLE `" table-name "` ADD COLUMN " (name col-name) " " (get-mysql-data-type type size decimals)))

(defn calculate-new-sizes [clm1 clm2]
  (let [{r-size :size r-decimals :decimals} clm1
        {l-size :size l-decimals :decimals} clm2]
    (-> clm1
        (assoc,, :size (max r-size l-size))
        (assoc,, :decimals (max r-decimals l-decimals)))))


(defn may-be-resize [{left :left, right :right}]
  (cond
    (nil? right) (add-column left)
    (or (not= (:size left) (:size right))
        (not= (:decimals left) (:decimals right))) (resize-column (calculate-new-sizes right left))
    :else nil))


(defn get-resize-queries [csv-columns db-columns]
  (keep may-be-resize (left-join-pair #(= (:name %1) (:name %2)) csv-columns db-columns)))


(defn resize-if-necessary! [ds record-map table-name]
  (let [db-columns (db/get-summarized-columns ds table-name)
        csv-columns (map #(assoc (weigh (val %1)) :name (key %1)) record-map)
        queries (get-resize-queries csv-columns db-columns)]
    (run! #(jdbc/execute-one! ds [%1]) queries)))



(defn insert-data! [ds csv str-table-name]
  (let [map-list (to-map-list csv)
        kw-table-name (keyword str-table-name)]
    (run! (fn [record-item]
            (do-with-retry #(sql/insert! ds kw-table-name record-item)
                           #(resize-if-necessary! ds record-item str-table-name))) map-list)))



(defn create-or-insert! [ds csv name]
  (let [table-record (db/get-tables ds name)
        table-count (count table-record)]
    (when (= 0 table-count)
      (create-table! ds csv name))
    (insert-data! ds csv name)))





; check if table exists
; create table
; for each record insert
; if fails to to adjust table
; insert again

