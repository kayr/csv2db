(ns fuzzy-sql.core-test
  (:require [clojure.test :refer :all]
            [fuzzy-sql.exporter :refer :all]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql])
  (:import (java.util UUID)))

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

(deftest test-detect-data-type
  (testing "detecting data type"
    (is (= (detect-data-type test-data "someNulls") BigDecimal))
    (is (= (detect-data-type test-data "header") String))
    (is (= (detect-data-type test-data "age") Long))))


(deftest test-detect-db-type
  (testing "detecting data type"
    (is (= (detect-db-type test-data "someNulls") "numeric(19,6)"))
    (is (= (detect-db-type test-data "header") "varchar(255)"))
    (is (= (detect-db-type test-data "age") "bigint"))))

(deftest test-generate-ddl
  (testing "testing generate ddl"
    (is (= (generate-ddl test-data "my_table")
           "create table `my_table` (`header` varchar(255),`age` bigint,`someNulls` numeric(19,6))"))))

(defn fetch-data [ds table]
  (sql/query ds [(str "select * from " table)] {:builder-fn next.jdbc.result-set/as-unqualified-arrays}))

(defn test-csv-insertion [csv]
  (let [rand-tb-name (rand-table-name!)]
    (create-or-insert! ds csv rand-tb-name)
    (let [db-data (fetch-data ds rand-tb-name)]
      (is (= (convert-to-key-csv csv) db-data)))))

(deftest test-insertion
  (testing "creating the table"
    (test-csv-insertion test-data)
    (test-csv-insertion test-data-large)
    ))









