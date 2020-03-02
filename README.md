# elasticsearch-copy

Copy docs from one elasticsearch index to another. Indices can be located on seperate Elasticsearch instances.

## Quickstart

### Prerequisites

- Minimum Java version `1.8`
- `clojure` cli tool

### Steps to run copy job

- Clone this repo and navigate to project root
- Update `config.edn` with your job paramaters.
- `clojure -m elasticsearch_copy.core`

## Options

Job parameters are provoded by an `edn` file.

- `source`: Job parameters for the source index, where the docs are being pulled from
  - `index`: Index to pull the docs from
  - `url`: URL of the Elasticsearch instance
  - `size`: Number of docs returned by each scroll query request; default is 500. The size should be smaller for datasources that have very large documents
  - `concurrency`: Number of concurrent scroll clients - should be less than or equal to the number of Elasticsearch data nodes. Default is 4
- `destination`: Job parameters for the destination index, where the docs from the source will be posted to
  - `index`: Index to post the docs to
  - `url`: URL of the Elasticsearch instance
  - `reporting-interval`: How often to print job status in miliseconds
  - `max-count`: Max number of docs to post at a time in a batch; default is 1,000. Will post this number of docs if `max-chars` has not been reached
  - `max-chars`: Max character count to post at a time in a batch; default is 2,000,000. The default character count keeps batches at about 1 MB when encoded in UTF-8, assuming mostly ASCII characters. The memory size for each batch is about 2 MB locally encoded as UTF-16, assuming mostly ASCII characters
  - `concurrency`: Maximum number of batches to be posted at a time. Default is 12. Setting to `source.concurrency` * 3 is generally most performant

## Examples

```clojure
{
 ;; Job parameters for the source index, where the docs are being pulled from
 :source
 {
  ;; Index to pull the docs from
  :index "abc123"

  ;; URL of the Elasticsearch instance
  :url "http://localhost:9200"

  ;; Number of docs returned by each scroll query request
  ;; Default is 500
  ;; The size should be smaller for datasources that have very large documents
  :size 500

  ;; Number of concurrent scroll clients
  ;; Should be less than or equal to the number of Elasticsearch data nodes
  ;; Default is 4
  :concurrency 4}

 ;; Job parameters for the destination index, where the docs from the source will be posted to
 :destination
 {
  ;; Index to post the docs to
  :index "def456"

  ;; URL of the Elasticsearch instance
  :url "http://localhost:9200"

  ;; How often to print job status in miliseconds
  ;; Default is 3000
  :report-interval 3000

  ;; Max number of docs to post at a time in a batch
  ;; default is 1,000
  ;; Will post this number of docs if `max-chars` has not been reached
  :max-count 1000

  ;; Max character count to post at a time in a batch
  ;; Default is 2,000,000
  ;; The default character count keeps batches at about 1 MB when encoded in UTF-8
  ;; assuming mostly ASCII characters
  ;; The memory size for each batch is about 2 MB locally encoded as UTF-16
  ;; assuming mostly ASCII characters
  :max-chars 2000000

  ;; Maximum number of batches to be posted at a time
  ;; Default is 12
  ;; Setting to `source/concurrency` * 3 is generally most performant
  :concurrency 12
  }}
```
