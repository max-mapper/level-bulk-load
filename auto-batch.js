var ldjstream = require('ldjson-stream')
var combiner = require('stream-combiner')
var through = require('through')
var level = require('level')
var uuid = require('hat')

module.exports = function(opts, cb) {
  if (!opts) opts = {
    writeBufferSize: 1024 * 1024 * 16, // mb
    blockSize: 1024 * 64 // kb
  }
  
  var currentBatch
  var batchPending = false
  var size = 0
  var batcher = through(batch, finish)
  var parser = combiner(ldjstream.parse(), batcher)
  var db = level('test.db', opts, function(err) {
    cb(err, parser)
  })
  
  function batch(obj) {
    // simplest way to get object size?
    var obj = JSON.stringify(obj)
    var len = obj.length
    if (!currentBatch) currentBatch = db.batch()
    // keep batches under write buffer size
    if (!batchPending && ((size + len) >= opts.writeBufferSize)) {
      // if single obj is bigger than write buffer
      if (size === 0) {
        console.log('big single obj', obj)
        currentBatch.put(uuid(), obj)
      }
      
      // ops length check is to prevent commit from firing multiple times
      // i'm not sure how batchPending is never true...
      if (currentBatch.ops.length > 0) commit()
    } else {
      currentBatch.put(uuid(), obj)
      size += len
    }
  }
  
  function finish() {
    var self = this
    if (currentBatch) {
      commit(function() {
        self.queue(null)
      })
    } else {
      self.queue(null)
    }
  }
  
  function commit(cb) {
    batchPending = true
    var time = +new Date()
    var stats = ', rows: ' + currentBatch.ops.length + ', bytes: ' + size
    batcher.pause()
    currentBatch.write(function(err) {
      if (err) console.log('batch err', err)
      console.log('batch time: ' + (+new Date() - time) + 'ms' + stats)
      currentBatch = undefined
      batchPending = false
      size = 0
      batcher.resume()
      if (cb) cb()
    })
  }
}
