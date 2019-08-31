(ns fuzzy-sql.core-test
  (:require [clojure.test :refer :all]
            [fuzzy-sql.exporter :refer :all]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql])
  (:import (java.util UUID Calendar)))

(defn repeat-string [n s] (->> (repeat n s) (reduce str)))

(defn rand-table-name! [] (str "T_" (-> (UUID/randomUUID)
                                        (str)
                                        (.replace "-" ""))))


(def test-data [["header" "age" "someNulls"]
                ["value 1" 3 nil]
                ["value 2" 4 3.4M]])

(def test-data-large [["header" "age" "someNulls"]
                      [(repeat-string 300 "x") 3 nil]
                      ["value 2" 4 3.4M]])
(defn now-to-sec!! []
  (.getTime (doto (Calendar/getInstance)
              (.set Calendar/MILLISECOND 0))))

(def test-data-large-date [["header" "age" "someNulls" "newColl", "new_col_3"]
                           ["hhh" 3 nil (now-to-sec!!) nil]
                           ["value 2" 4 3.4M nil 3.9999877283M]])

(defn convert-to-key-csv [[header & body]]
  (cons (map #(keyword %1) header) body))




(def db {:dbtype   "mysql"
         :dbname   "dsd"
         :user     "root"
         :password "pass"
         :host     "localhost"
         :useSSl   false})

(def ds (jdbc/get-datasource db))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 1 1))))

(deftest test-index-of
  (let [data [:a :b :c :d]
        c-index (fuzzy-sql.utils/index-of #(= :c %1) data)]
    (is (= c-index 2))))

(defn detect-data-type [csv name]
  (type (get-first-value csv name)))

(deftest test-detect-data-type
  (testing "detecting data type"
    (is (= (detect-data-type test-data "someNulls") BigDecimal))
    (is (= (detect-data-type test-data "header") String))
    (is (= (detect-data-type test-data "age") Long))))


(deftest test-detect-db-type
  (testing "detecting data type"
    (is (= (detect-db-type test-data "someNulls") "numeric(2,1)"))
    (is (= (detect-db-type test-data "header") "varchar(7)"))
    (is (= (detect-db-type test-data-large-date "newColl") "datetime"))
    (is (= (detect-db-type test-data-large-date "new_col_3") "numeric(11,10)"))
    (is (= (detect-db-type test-data "age") "bigint"))))

(deftest test-generate-ddl
  (testing "testing generate ddl"
    (is (= (generate-ddl test-data "my_table")
           "create table `my_table` (`header` varchar(7),`age` bigint,`someNulls` numeric(2,1))"))))

(defn fetch-data [ds table]
  (sql/query ds [(str "select * from " table)] {:builder-fn next.jdbc.result-set/as-unqualified-arrays}))

(defn assert-data-eq [table-name csv]
  (let [db-data (fetch-data ds table-name)]
    (is (= (convert-to-key-csv csv) db-data))))


(defn test-csv-insertion [csv rand-tb-name]
  (create-or-insert! ds csv rand-tb-name)
  (assert-data-eq rand-tb-name csv))


;
(deftest test-insertion
  (testing "creating the table"
    (test-csv-insertion test-data (rand-table-name!))
    (test-csv-insertion test-data-large (rand-table-name!))
    (test-csv-insertion test-data-large-date (rand-table-name!))))
;
;
(deftest test-insertion-resize
  (testing "creating the table"
    (let [table-name "resizable_table"]
      (jdbc/execute! ds [(str "drop table if exists " table-name)])
      (create-or-insert! ds test-data table-name)
      (create-or-insert! ds test-data-large table-name)
      (create-or-insert! ds test-data-large-date table-name)
      (is (= (count (fetch-data ds table-name)) 7)))))









