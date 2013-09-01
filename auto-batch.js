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
      console.log('batch filled from new obj', size, len)
      if (currentBatch.ops.length > 0) commit()
    } else {
      // try to fill up the batch from queued objs
      for (var i = 0; i < pending.length; ++i) {
        if (batchPending) continue
        var data = pending.shift()
        currentBatch.put(uuid(), data.obj)
        console.log('put from pending')
        size += data.len
        if (size >= opts.writeBufferSize) {
          console.log('batch filled from pending obj')
          commit()
        }
      }
      if (batchPending) {
        // queue incoming object
        pending.push({len: len, obj: obj})
        console.log('pending length:', pending.length)
      } else {
        currentBatch.put(uuid(), obj)
        size += len
        console.log('put from new, new size:', size)
        if (size >= opts.writeBufferSize) {
          console.log('batch filled from new obj + pending')
          commit()
        }
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
    console.time('batch time')
    batcher.pause()
    console.log('num rows:', currentBatch.ops.length, 'bytes:', size)
    currentBatch.write(function(err) {
      if (err) console.log('batch err', err)
      console.timeEnd('batch time')
      currentBatch = undefined
      batchPending = false
      batcher.resume()
      size = 0
      if (cb) cb()
    })
  }
}
