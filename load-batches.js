var level = require('level')
var fs = require('fs')
var path = require('path')

var data = fs.readFileSync(path.join(__dirname, '10kilobytesof.json')).toString()

var bufferSize = 16 // megabytes
var size = 100 * bufferSize
var batches = process.argv[2] || 25

console.log('loading', batches, 'batches of', size)

var db = level(path.join(process.cwd(), 'test.db'), {
  writeBufferSize: 1024 * 1024 * bufferSize
}, load)

function load() {
  batches--
  if (batches <= 0) return
  putBatch(load)
}

function putBatch(cb) {
  console.time('batch of ' + size)
  var batch = db.batch()
  for (var i = 0; i < size; i++) batch.put(i + '-' + +new Date(), data)
  batch.write(function() {
    console.timeEnd('batch of ' + size)
    cb()
  })
}


