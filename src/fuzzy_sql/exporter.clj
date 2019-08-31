(ns fuzzy-sql.exporter
  (:require
    [fuzzy-sql.db :as db]
    [next.jdbc :as jdbc]
    [fuzzy-sql.utils :as utils]
    [next.jdbc.sql :as sql])
  (:import (java.util Date)))

(declare weigh gen-col-type)


(def MYSQL-TO-JAVA-MAPPINGS {"varchar"  [String]
                             "bigint"   [Integer Long BigInteger]
                             "numeric"  [Float Double BigDecimal]
                             "datetime" [java.sql.Date Date]})


(def JAVA-TO-SQL-DB-MAPPING (utils/unwind-values-to-keys MYSQL-TO-JAVA-MAPPINGS))


(defn get-header-index [csv name]
  (utils/index-of #(= %1 name) (first csv)))


(defn get-column [csv col-name]
  (let [index (get-header-index csv col-name)
        [_ & data] csv
        col-values (map #(nth %1 index) data)] col-values))


(defn get-first-value [csv name]
  (first (filter some? (get-column csv name))))


(defn detect-db-type [csv name]
  (let [firs-value (get-first-value csv name)
        db-type (JAVA-TO-SQL-DB-MAPPING (type (get-first-value csv name)))]
    (if (nil? firs-value) "varchar(255)")
    (gen-col-type db-type (weigh firs-value))))


(defn generate-ddl [csv name]
  (let [headers (first csv)
        col-expr (map #(str "`" %1 "` " (detect-db-type csv %1)) headers)
        all-col-exprs (reduce #(str %1 "," %2) col-expr)]
    (str "create table `" name "` (" all-col-exprs ")")))


(defn to-map-list [csv]
  (let [[header & data] csv]
    (map #(zipmap header %1) data)))


(defn weigh [v]
  (cond
    (instance? String v) {:size (count v) :decimals 0}

    (instance? Number v) (let [[size decimals] (.split (str v) "\\.")]
                           {:size (count size) :decimals (count (or decimals ""))})

    (instance? Date v) {:size 0 :decimals 0}

    :else {:size (count (str v)) :decimals 0}))


(defn create-table! [ds csv name]
  (jdbc/execute! ds [(generate-ddl csv name)]))


(defmulti gen-col-type (fn [data-type _] data-type))

(defmethod gen-col-type "datetime" [data-type _] data-type)

(defmethod gen-col-type "bigint" [data-type _] data-type)

(defmethod gen-col-type "numeric" [data-type {:keys [size decimals]}]
  (str data-type "(" (+ size decimals) "," decimals ")"))

(defmethod gen-col-type :default [data-type {:keys [size decimals]}]
  (str data-type "(" (if (> decimals 0) (str size "," decimals) (str size)) ")"))



(defn get-mysql-data-type [type size decimals]
  (if (or (.equalsIgnoreCase "numeric" type)
          (.equalsIgnoreCase "decimal" type))
    (str type "(" (str (+ decimals size) "," decimals) ")")
    (str type "(" size ")")))


(defn resize-column [{col-name :name :keys [size decimals :type table-name]}]
  (str "ALTER TABLE `" table-name "` MODIFY `" (name col-name) "` " (get-mysql-data-type type size decimals)))


(defn add-column [{col-name :name :keys [full-db-type]} table-name]
  (str "ALTER TABLE `" table-name "` ADD COLUMN `" (name col-name) "` " full-db-type))


(defn calculate-new-sizes [clm1 clm2]
  (let [{r-size :size r-decimals :decimals} clm1
        {l-size :size l-decimals :decimals} clm2]
    (assoc clm1 :size (max r-size l-size)
                :decimals (max r-decimals l-decimals))))


(defn may-be-resize [{csv-column :left, db-column :right} table-name]
  (cond
    (nil? db-column) (add-column csv-column table-name)
    (or (> (:size csv-column) (:size db-column))
        (> (:decimals csv-column) (:decimals db-column))) (resize-column (calculate-new-sizes db-column csv-column))
    :else nil))


(defn- get-resize-queries [csv-columns db-columns table-name]
  (keep #(may-be-resize %1 table-name)
        (utils/left-join-pair #(= (:name %1) (:name %2)) csv-columns db-columns)))


(defn- resize-if-necessary! [ds csv record-map table-name]
  (let [db-columns (db/get-summarized-columns ds table-name)
        csv-columns (map #(assoc (weigh (val %1)) :name (key %1)
                                                  :full-db-type (detect-db-type csv (key %1)))
                         record-map)
        queries (get-resize-queries csv-columns db-columns table-name)]
    (run! #(do (println %1)
               (jdbc/execute-one! ds [%1])) queries)))



(defn insert-data! [ds csv str-table-name]
  (let [map-list (to-map-list csv)
        kw-table-name (keyword str-table-name)]
    (run! (fn [record-item]
            (utils/do-with-retry #(sql/insert! ds kw-table-name record-item)
                                 #(resize-if-necessary! ds csv record-item str-table-name))) map-list)))


(defn create-or-insert! [ds csv name]
  (let [table-records (db/get-tables ds name)
        table-count (count table-records)]
    (when (= 0 table-count) (create-table! ds csv name))
    (insert-data! ds csv name)))



