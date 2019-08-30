(ns fuzzy-sql.utils)

(defn create-map-vk [[k values]]
  (if (coll? values)
    (reduce (fn [a v] (assoc a v k)) {} values)
    {values k}))

(defn unwind-values-to-keys [data-map]
  (reduce (fn [acc v] (into (create-map-vk v) acc)) {} data-map))


(defn filter-and-pair-with-index [pred? coll]
  (let [data-and-index (map-indexed vector coll)
        filtered-item-pairs (filter (fn [[_ item]] (pred? item)) data-and-index)]
    filtered-item-pairs))

(defn index-of [pred? coll]
  (let [[first-pair] (filter-and-pair-with-index pred? coll)
        [index] first-pair] index))



(defn do-with-retry [do-fn correct-fn]
  (try (do-fn)
       (catch Exception _
         (do (correct-fn) (do-fn)))))

(defn left-join-pair [join-fn col1 col2]
  (map (fn [val1] {:left  val1
                   :right (some (fn [val2] (when (join-fn val1 val2) val2)) col2)}) col1))
