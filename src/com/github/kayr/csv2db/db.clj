(ns com.github.kayr.csv2db.db
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))


(defn with-connection [ds fn-con-consumer]
  (with-open [c (jdbc/get-connection ds)] (fn-con-consumer c)))


(defn get-columns [ds table]
  (with-connection ds (fn [c]
                        (-> (.getMetaData c)
                            (.getColumns nil nil table nil)
                            (rs/datafiable-result-set ds {})))))

(defn- correct-size [col]
  (if (> (or (:decimals col) 0) 0)
    (assoc col :size (- (:size col) (:decimals col))) col))


(defn get-summarized-columns [ds table-name]
  (map #(correct-size {:size       (:COLUMN_SIZE %1)
                       :decimals   (or (:DECIMAL_DIGITS %1) 0)
                       :name       (:COLUMN_NAME %1)
                       :type       (:TYPE_NAME %1)
                       :table-name table-name})
       (get-columns ds table-name)))



(defn get-tables [ds table]
  (with-connection ds (fn [c]
                        (-> (.getMetaData c)
                            (.getTables nil nil table (into-array String ["TABLE"]))
                            (rs/datafiable-result-set ds {})))))



;[{:IS_NULLABLE "YES",
;  :IS_GENERATEDCOLUMN "NO",
;  :COLUMN_NAME "header",
;  :SQL_DATA_TYPE 0,
;  :COLUMN_DEF nil,
;  :TABLE_CAT "dsd",
;  :SCOPE_CATALOG nil,
;  :SQL_DATETIME_SUB 0,
;  :DECIMAL_DIGITS nil,
;  :COLUMN_SIZE 255,
;  :REMARKS "",
;  :BUFFER_LENGTH 65535,
;  :TYPE_NAME "VARCHAR",
;  :ORDINAL_POSITION 1,
;  :SOURCE_DATA_TYPE nil,
;  :NUM_PREC_RADIX 10,
;  :CHAR_OCTET_LENGTH 255,
;  :SCOPE_SCHEMA nil,
;  :SCOPE_TABLE nil,
;  :NULLABLE 1,
;  :TABLE_SCHEM nil,
;  :IS_AUTOINCREMENT "NO",
;  :TABLE_NAME "table1",
;  :DATA_TYPE 12}
; {:IS_NULLABLE "YES",
;  :IS_GENERATEDCOLUMN "NO",
;  :COLUMN_NAME "age",
;  :SQL_DATA_TYPE 0,
;  :COLUMN_DEF nil,
;  :TABLE_CAT "dsd",
;  :SCOPE_CATALOG nil,
;  :SQL_DATETIME_SUB 0,
;  :DECIMAL_DIGITS 0,
;  :COLUMN_SIZE 19,
;  :REMARKS "",
;  :BUFFER_LENGTH 65535,
;  :TYPE_NAME "BIGINT",
;  :ORDINAL_POSITION 2,
;  :SOURCE_DATA_TYPE nil,
;  :NUM_PREC_RADIX 10,
;  :CHAR_OCTET_LENGTH nil,
;  :SCOPE_SCHEMA nil,
;  :SCOPE_TABLE nil,
;  :NULLABLE 1,
;  :TABLE_SCHEM nil,
;  :IS_AUTOINCREMENT "NO",
 ;  :TABLE_NAME "table1",
;  :DATA_TYPE -5}
; {:IS_NULLABLE "YES",
;  :IS_GENERATEDCOLUMN "NO",
;  :COLUMN_NAME "someNulls",
;  :SQL_DATA_TYPE 0,
;  :COLUMN_DEF nil,
;  :TABLE_CAT "dsd",
;  :SCOPE_CATALOG nil,
;  :SQL_DATETIME_SUB 0,
;  :DECIMAL_DIGITS 6,
;  :COLUMN_SIZE 19,
;  :REMARKS "",
;  :BUFFER_LENGTH 65535,
;  :TYPE_NAME "DECIMAL",
;  :ORDINAL_POSITION 3,
;  :SOURCE_DATA_TYPE nil,
;  :NUM_PREC_RADIX 10,
;  :CHAR_OCTET_LENGTH nil,
;  :SCOPE_SCHEMA nil,
;  :SCOPE_TABLE nil,
;  :NULLABLE 1,
;  :TABLE_SCHEM nil,
;  :IS_AUTOINCREMENT "NO",
;  :TABLE_NAME "table1",
;  :DATA_TYPE 3}]