(ns word-count
  (:require
   [com.rpl.rama :as rama :refer [defmodule declare-depot declare-pstate stream-topology <<sources source> |hash +compound]]
   [com.rpl.rama.path :refer [keypath]]
   [com.rpl.rama.ops :as ops]
   [com.rpl.rama.aggs :as aggs]
   [clojure.string :as str]
   [com.rpl.rama.test :as rtest]))

(defmodule WordCountModule [setup topologies]
           (declare-depot setup *sentences-depot :random)
           (let [s (stream-topology topologies "word-count")]
             (declare-pstate s $$word-counts {String Long})
             (<<sources s
               (source> *sentences-depot :> *sentence)
               (str/split (str/lower-case *sentence) #" " :> *words)
               (ops/explode *words :> *word)
               (|hash *word)
               (+compound $$word-counts {*word (aggs/+count)}))))

(defn run-module! [_]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc WordCountModule {:tasks 4 :threads 2})

    (let [sentences-depot (rama/foreign-depot ipc (rama/get-module-name WordCountModule) "*sentences-depot")
          word-counts     (rama/foreign-pstate ipc (rama/get-module-name WordCountModule) "$$word-counts")]

      (rama/foreign-append! sentences-depot "Hello world")
      (rama/foreign-append! sentences-depot "hello hello goodbye")
      (rama/foreign-append! sentences-depot "Alice says hello")

      (println "hello:" (rama/foreign-select-one (keypath "hello") word-counts))
      (println "goodbye:" (rama/foreign-select-one (keypath "goodbye") word-counts)))))
