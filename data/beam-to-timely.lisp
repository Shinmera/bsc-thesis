#!sbcl --script
;;;;; Beam to Timely Benchmark Converter
;;;; This script converts the output of a Beam run into a JSON file
;;;; usable as input for the Timely Benchmark system.
;;;;
;;;; Usage: ./beam-to-timely.lisp input-file [output-file]
;;;;
;;;; Example beam run to generate the data:
;;;;   mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pflink-runner -Dexec.args="--runner=FlinkRunner --suite=SMOKE --streaming=false --manageResources=false --monitorJobs=true --flinkMaster=local --parallelism=1 --numEventGenerators=1 --sinkType=TEXT --outputPath=."
;;;;
;;;; Example conversion invocation:
;;;;   ./beam-to-timely.lisp nexmark_Query0_1524153175427.txt-00000-of-00001 nexmark.json
;;;;
;;;; Requires a Quicklisp installation in the user's home directory.
;;;;
(load #p"~/quicklisp/setup.lisp")
(ql:quickload '(yason local-time cl-ppcre))

(defun table (&rest values)
  (let ((table (make-hash-table :test 'equal)))
    (loop for (k v) on values by #'cddr
          do (setf (gethash k table) v))
    table))

(defun convert-event (event)
  (flet ((val (key) (gethash key event)))
    (cond ((val "name")
           (table "type" "Person"
                  "id" (val "id")
                  "name" (val "name")
                  "email_address" (val "emailAddress")
                  "credit_card" (val "creditCard")
                  "city" (val "city")
                  "state" (val "state")
                  "date_time" (val "dateTime")))
          ((val "itemName")
           (table "type" "Auction"
                  "id" (val "id")
                  "item_name" (val "itemName")
                  "description" (val "description")
                  "initial_bid" (val "initialBid")
                  "reserve" (val "reserve")
                  "date_time" (val "dateTime")
                  "expires" (val "expires")
                  "seller" (val "seller")
                  "category" (val "category")))
          ((val "bidder")
           (table "type" "Bid"
                  "auction" (val "auction")
                  "bidder" (val "bidder")
                  "price" (val "price")
                  "date_time" (val "dateTime")))
          (T
           (error "Unknown event type:~% ~s" event)))))

(defun convert-file (source target &key (if-exists :supersede))
  (with-open-file (in source :direction :input)
    (with-open-file (out target :direction :output :if-exists if-exists)
      (loop for line = (read-line in NIL)
            while line
            do (cl-ppcre:register-groups-bind (json NIL) ("TimestampedValue\\((.+), ([\\d\\w-:.]+)\\)" line)
                 (let* ((json (yason:parse json))
                        (time (local-time:unix-to-timestamp (round (/ (gethash "dateTime" json) 1000))))
                        (epoch (- (local-time:timestamp-to-universal time)
                                  (encode-universal-time 0 0 0 15 7 2015 0))))
                   (yason:encode
                    (table
                     "time" epoch
                     "event" (convert-event json))
                    out)
                   (terpri out))))
      target)))

(defun toplevel (&rest args)
  (convert-file (first args) (or (second args)
                                 "output.json")))

(apply #'toplevel (rest (uiop:raw-command-line-arguments)))
