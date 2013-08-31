var ldjstream = require('ldjson-stream')
var combiner = require('stream-combiner')
var through = require('through')
var level = require('level')
var uuid = require('hat')

module.exports = function(opts, cb) {
  if (!opts) opts = {
    writeBufferSize: 1024 * 1024 * 1, // default in leveldown
    blockSize: 4096 // default in leveldown
  }
  
  var currentBatch
  var batchPending = false
  var pending = []
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
    console.log(len, size, opts.writeBufferSize, batchPending)
    if (!currentBatch) currentBatch = db.batch()
    // keep batches under write buffer size
    if (!batchPending && ((size + len) >= opts.writeBufferSize)) {
      // if single obj is bigger than write buffer
      if (size === 0) currentBatch.put(uuid(), obj)
      commit()
    } else {
      // try to fill up the batch from queued objs
      for (var i = 0; i < pending.length; ++i) {
        if (batchPending) continue
        var data = pending.shift()
        currentBatch.put(uuid(), data.obj)
        size += data.len
        if (size >= opts.writeBufferSize) {
          commit()
        }
      }
      if (batchPending) {
        pending.push({len: len, obj: obj})
      } else {
        currentBatch.put(uuid(), obj)
        size += len
        if (size >= opts.writeBufferSize) commit()
      }
    }
  }
  
  function finish() {
    var self = this
    if (currentBatch) {
      for (var i = 0; i < pending.length; ++i) {
        var data = pending.shift()
        currentBatch.put(uuid(), data.obj)
      }
      commit(function() {
        self.queue(null)
      })
    } else {
      self.queue(null)
    }
  }
  
    
  function commit(cb) {
    batchPending = true
    console.time('pendingbatch')
    batcher.pause()
    console.log('writing batch length', currentBatch.ops.length, 'size', size)
    currentBatch.write(function() {
      console.timeEnd('pendingbatch')
      currentBatch = undefined
      batchPending = false
      batcher.resume()
      size = 0
      if (cb) cb()
    })
  }
}
