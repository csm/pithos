(ns io.pithos.blob-s3
  "Crazypants implementation of a blobstore on S3."
  (require [io.pithos.blob :refer [Blobstore ProxiedBlobstore]]
           [io.pithos.store :as store]
           [io.pithos.desc :as d]
           [amazonica.aws.s3 :as s3]
           [clojure.string :refer [join]]
           [clojure.tools.logging :refer [debug info error]])
  (import [java.io InputStream ByteArrayInputStream OutputStream]
          [java.nio BufferUnderflowException ByteBuffer]
          [java.util UUID]
          [com.amazonaws.services.s3.model AmazonS3Exception]
          [com.google.common.io ByteStreams CountingInputStream]))

(defn buffer->stream
  "Turn a byte buffer into an input stream."
  [^ByteBuffer buffer]
  (if (.hasArray buffer)
    (ByteArrayInputStream. (.array buffer) (.position buffer) (.remaining buffer))
    (proxy [InputStream] []
        (read
         ([]
          (try
            (bit-and (.get buffer) 0xff)
            (catch BufferUnderflowException _ -1)))
         ([buf off len]
          (let [length (min (.remaining buffer) len)]
            (.get buffer buf off length)
            length))))))

(defn stream->buffer
  "Turn a stream into a byte buffer."
  [stream]
  (ByteBuffer/wrap (ByteStreams/toByteArray stream)))

(def empty-stream
  (proxy [InputStream] []
    (read
     ([] -1)
     ([buf off len] 0))))

(defn s3-blobstore
  [{:keys [endpoint access-key secret-key profile bucket chunk-size] :as conf}]
  (let [creds (select-keys conf [:endpoint :access-key :secret-key :profile])
        bucket (:bucket conf)
        chunk-size (or (:chunk-size conf) (* 5 1024 1024))
        inode (fn [od] (str (d/inode od)))
        version (fn [od] (str (d/version od)))]
    (reify
      store/Convergeable
      (converge! [this]
        (try
          (s3/create-bucket :bucket-name bucket :region endpoint)
          (catch AmazonS3Exception e
            (if (= (.getErrorCode e) "BucketAlreadyOwnedByYou")
              nil
              (throw e)))))

      store/Crudable
      (delete! [this od version]
        (debug "delete" od version)
        (try
          (s3/delete-object creds
                            :bucket-name bucket
                            :key (str (if (instance? UUID od) od (d/inode od)) "/" (str version)))
          (catch AmazonS3Exception e
            (info "exception when deleting" od version "--" e))))

      ProxiedBlobstore
      (proxied-bucket [this] {:name bucket})

      (proxy-get [this od out [start end]]
        (debug "streaming out" (inode od) (version od) start end)
        (let [obj (s3/get-object creds
                                 :bucket-name bucket
                                 :key (str (inode od) "/" (version od))
                                 :range [start end])]
          (ByteStreams/copy (:input-stream obj) out)))
      
      (proxy-put! [this od in]
        (debug "streaming in" (inode od) "version" (version od))
        (let [key (str (inode od) "/" (version od))
              mp-request (s3/initiate-multipart-upload creds
                                                       :bucket-name bucket
                                                       :key key)]
          (loop [part 1
                 part-etags []]
            (let [buffer (ByteStreams/toByteArray (ByteStreams/limit in chunk-size))
                  cnt (alength buffer)]
              (debug "uploading part" part "of size" cnt)
              (let [upload-part (s3/upload-part creds
                                                :upload-id (:upload-id mp-request)
                                                :bucket-name bucket
                                                :key key
                                                :input-stream (ByteArrayInputStream. buffer 0 cnt)
                                                :part-number part
                                                :part-size cnt
                                                :is-last-part (< cnt chunk-size))]
                (debug "upload part result" upload-part)
                (if (< cnt chunk-size)
                  (do
                    (debug "completing multipart upload with part-etags" part-etags)
                    (s3/complete-multipart-upload creds
                                                  :bucket-name bucket
                                                  :key key
                                                  :upload-id (:upload-id mp-request)
                                                  :part-etags (conj part-etags (:part-etag upload-part))))
                  (recur (inc part)
                         (conj part-etags (:part-etag upload-part)))))))))

      (proxy-copy! [this src src-bucket dst]
        (debug "copying" src "in" src-bucket "to" dst)
        (s3/copy-object :source-bucket-name src-bucket
                        :source-key (str (inode src) "/" (version dst))
                        :destination-bucket-name bucket
                        :destination-key (str (inode dst) "/" (version dst))))

      (proxy-copy-parts! [this parts dst notifier]
        (let [mp-request (s3/initiate-multipart-upload creds
                                                       :bucket-name bucket
                                                       :key (str (inode dst) "/" (version dst)))
              part-etags (map (fn [[part-number [src src-bucket]]]
                                (notifier :block)
                                (debug "copying part" (str (inode src) "/" (version src))
                                       "in" src-bucket "to" (str (inode dst) "/" (version dst)))
                                (:part-etag
                                 (s3/copy-part creds
                                               :upload-id (:upload-id mp-request)
                                               :destination-bucket-name bucket
                                               :destination-key (str (inode dst) "/" (version dst))
                                               :part-number (inc part-number)
                                               :source-bucket-name src-bucket
                                               :source-key (str (inode src) "/" (version src)))))
                              (map-indexed vector parts))]
            (s3/complete-multipart-upload creds
                                          :bucket-name bucket
                                          :key (str (inode dst) "/" (version dst))
                                          :upload-id (:upload-id mp-request)
                                          :part-etags (into [] (doall part-etags))))
          (let [obj-meta (s3/get-object-metadata creds
                                                 :bucket-name bucket
                                                 :key (str (inode dst) "/" (version dst)))]
            [(:content-length obj-meta) (:etag obj-meta)])))))
