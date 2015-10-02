(ns io.pithos.stream
  "Read and write to cassandra from OutputStream and InputStream"
  (:import java.io.OutputStream
           java.io.InputStream
           java.nio.ByteBuffer
           org.eclipse.jetty.server.HttpInputOverHTTP
           javax.servlet.ReadListener
           com.google.common.io.CountingInputStream
           java.security.DigestInputStream)
  (:require [io.pithos.blob :as b]
            [io.pithos.desc :as d]
            [io.pithos.util :as u]
            [clojure.tools.logging :refer [debug error]]))

(defn chunk->ba
  "Chunks in pithos come back as bytebuffers and we
   need byte-arrays for outputstreams, this converts
   from the former to the latter."
  [{:keys [payload]}]
  (let [array (.array payload)
        off   (.position payload)
        len   (- (.limit payload) off)]
    [array off len]))

(defn stream-to
  "Given an outputstream and a range, stream from
   an object descriptor to that outputstream."
  [od ^OutputStream stream [start end]]
  (debug "got range: " start end)
  (let [blob (d/blobstore od)]
    (if (satisfies? b/ProxiedBlobstore blob)
      (b/proxy-get blob od stream [start end])
      (let [blocks (b/blocks blob od)]
        (debug "got " (count blocks) "blocks" blocks "end:" end)
        (try
          (doseq [{:keys [block]} blocks
                  :while (<= block end)]
            (debug "found block " block)
            (loop [offset block]
              (when-let [chunks (seq (b/chunks (d/blobstore od) od block offset))]
                (debug "got " (count chunks) " chunks")
                (doseq [{:keys [offset chunksize] :as chunk}  chunks
                        :let [[array off len] (chunk->ba chunk)]
                        :while (<= offset end)]
                  (let [start-at (if (<= offset start chunksize)
                                   (- start offset)
                                   0)
                        end-at   (if (<= offset end chunksize)
                                   (- end offset)
                                   len)
                        cropped  (- len start-at (- len end-at))]
                    (.write stream array (+ off start-at) cropped)))
                (let [{:keys [offset chunksize]} (last chunks)]
                  (recur (+ offset chunksize))))))
          (catch Exception e
            (error e "error during read"))
          (finally
            (debug "closing after read")
            (.flush stream)
            (.close stream)))))
    od))

(defn stream-from
  "Given an input stream and an object descriptor, stream data from the
   input stream to the descriptor.

   Our current approach has the drawback of not enforcing blocksize
   requirements since we have no way of being notified when reaching a
   threshold."
  [^InputStream stream od]
  (let [blob   (d/blobstore od)
        hash   (u/md5-init)]
    (try
      (if (satisfies? b/ProxiedBlobstore blob)
        (let [counting-stream (CountingInputStream. stream)
              digest-stream (DigestInputStream. counting-stream hash)]
          (b/proxy-put! blob od digest-stream)
          (d/col! od :size (.getCount counting-stream))
          (d/col! od :checksum (u/md5-sum hash))
          od)
        (loop [block  0
               offset 0]
          (when (>= block offset)
            (debug "marking new block")
            (b/start-block! blob od block))

          (let [chunk-size (b/max-chunk blob)
                ba         (byte-array chunk-size)
                br         (.read stream ba)]
            (if (neg? br)
              (do
                (debug "negative write, read whole stream")
                (d/col! od :size offset)
                (d/col! od :checksum (u/md5-sum hash))
                od)
              (let [chunk  (ByteBuffer/wrap ba 0 br)
                    sz     (b/chunk! blob od block offset chunk)
                    offset (+ sz offset)]
                (u/md5-update hash ba 0 br)
                (if (b/boundary? blob block offset)
                  (recur offset offset)
                  (recur block offset)))))))
      (catch Exception e
        (error e "error during write"))
      (finally
        (debug "closing after write")
        (.close stream)))))

(defn stream-copy
  "Copy from one object descriptor to another."
  [src dst]
  (let [sblob (d/blobstore src)
        dblob (d/blobstore dst)]
    (if (and (satisfies? b/ProxiedBlobstore sblob)
             (satisfies? b/ProxiedBlobstore dblob))
      (b/proxy-copy! dblob src (:name (b/proxied-bucket src)) dst)
      (let [blocks (b/blocks sblob src)]
        (doseq [{:keys [block]} blocks]
          (b/start-block! dblob dst block)
          (debug "found block " block)
          (loop [offset block]
            (when-let [chunks (seq (b/chunks sblob src block offset))]
              (doseq [chunk chunks
                      :let [offset (:offset chunk)]]
                (b/chunk! dblob dst block offset (:payload chunk)))
              (let [{:keys [offset chunksize]} (last chunks)]
                (recur (+ offset chunksize))))))))
    (d/col! dst :size (d/size src))
    (d/col! dst :checksum (d/checksum src))
    dst))

(defn stream-copy-part-block
  "Copy a single part's block to a destination"
  [notifier dst hash part g-offset {:keys [block]}]
  (let [dblob      (d/blobstore dst)
        sblob      (d/blobstore part)
        real-block (+ g-offset block)]
    (debug "streaming block: " block)
    (b/start-block! dblob dst real-block)
    (notifier :block)
    (last
     (for [chunk (b/chunks sblob part block block)
           :let [offset      (:offset chunk)
                 payload     (:payload chunk)
                 real-offset (+ g-offset offset)]]
       (do
         (b/chunk! dblob dst real-block real-offset payload)
         (notifier :chunk)
         (let [pos (.position payload)
               sz  (.remaining payload)
               ba  (byte-array sz)]
           (.get payload ba)
           (.position payload pos)
           (u/md5-update hash ba 0 sz)
           (+ real-offset sz)))))))


(defn stream-copy-part
  "Copy a single part to a destination"
  [notifier dst [offset hash] part]
  (let [sblob  (d/blobstore part)
        blocks (b/blocks sblob part)]

    (debug "streaming part: " (d/part part))
    [(reduce (partial stream-copy-part-block notifier dst hash part)
             offset blocks)
     hash]))

(defn stream-copy-parts
  "Given a list of parts, stream their content to a destination inode"
  [parts dst notifier]
  (let [dblob (d/blobstore dst)]
    (if (satisfies? b/ProxiedBlobstore dblob)
      (let [[size hash] (b/proxy-copy-parts! dblob
                                             (map (fn [part] [part (:name (b/proxied-bucket (d/blobstore part)))]) parts)
                                             dst
                                             notifier)]
        (d/col! dst :size size)
        (d/col! dst :checksum hash)
        dst)
      (let [[size hash] (reduce (partial stream-copy-part notifier dst)
                                [0 (u/md5-init)] parts)]
        (d/col! dst :size size)
        (d/col! dst :checksum (u/md5-sum hash))
        (debug "stored size:" size "and checksum: " (u/md5-sum hash))
        dst))))
